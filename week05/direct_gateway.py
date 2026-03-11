#Application semantics (direct messaging)
#A conversation is defined by two user ids: user_a = min(u1,u2), user_b = max(u1,u2)
#Writes are ordered via the Raft log; reads must reflect committed/applied state
#Client retries must not create duplicates:
#Each send includes (client_id, client_msg_id)
#Your state machine must deduplicate on this pair


import grpc
from generated import direct_gateway_pb2
from generated import direct_gateway_pb2_grpc
from generated import replica_admin_pb2
from generated import replica_admin_pb2_grpc
import time
import threading
from concurrent import futures




#Required network topology (single machine, ports)
#Default expected ports:
#Gateway: 127.0.0.1:50051
#Replicas (5):
#127.0.0.1:50061
#127.0.0.1:50062
#127.0.0.1:50063
#127.0.0.1:50064
#127.0.0.1:50065
#All addresses must remain configurable as host:port.

REPLICAS = [
    "127.0.0.1:50061",
    "127.0.0.1:50062",
    "127.0.0.1:50063",
    "127.0.0.1:50064",
    "127.0.0.1:50065",
]


def find_leader(replicas):
     for addr in replicas: #loop through all the addresess of the replicas
         try: # try to connect to the replicas using the replica admin
             stub = replica_admin_pb2_grpc.ReplicaAdminStub(grpc.insecure_channel(addr))
             r = stub.Status(replica_admin_pb2.StatusRequest(), timeout=2) #  send a status request to the replica admin if no answer in 2 seconds it will skip to the next replica
             if r.role == replica_admin_pb2.LEADER: #check if the status is leader if it is return the address of the replica if not go to the next replica
                return addr
         except:
             continue
     return None #no replica is available

class DirectGatewayServicer(direct_gateway_pb2_grpc.DirectGatewayServicer):
    def __init__(self):
        self.dupe = {} #to check dupes
        self.lock = threading.Lock() #if there are multiple threads trying to access the dic at the same time lock it so only one thread can access it at a time
    
    def SendDirect(self, request, context):
        #send message to replica
        #return a response
        dupe_key = (request.client_id, request.client_msg_id)
        with self.lock:
            if dupe_key in self.dupe: #check if the key is in the dic if it is return a response with the seq number of the dupe
                return direct_gateway_pb2.SendDirectResponse(seq=self.dupe[dupe_key])
        leader = find_leader(REPLICAS)
        if leader == None: #if there is no leader return an error
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No leader available")
            return direct_gateway_pb2.SendDirectResponse()
        try: 
            stub = replica_admin_pb2_grpc.ReplicaAdminStub(grpc.insecure_channel(leader)) #connect to the leader
            
            user_a = min(request.from_user, request.to_user)
            user_b = max(request.from_user, request.to_user)

            resp = stub.ClientRequest(
                replica_admin_pb2.ClientRequestMsg(
                    from_user = user_a,
                    to_user = user_b,
                    client_id = request.client_id,
                    client_msg_id = request.client_msg_id,
                    text = request.text
                ),
                timeout =5
            )

            if not resp.success:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("Leader is unavailable")
                return direct_gateway_pb2.SendDirectResponse()
            with self.lock:
                self.dupe[dupe_key] = resp.seq #save the sequence number to the dupe dictionary to prevent duplicates
            return direct_gateway_pb2.SendDirectResponse(seq = resp.seq) #return the sequence number to the client
        except grpc.RpcError as e:#check for errors in the rpc call
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Leader is unavailable")
            return direct_gateway_pb2.SendDirectResponse()
        
    def GetConversationHistory(self, request, context):
        #A conversation is defined by two user ids
        user_a = min(request.user_a, request.user_b)  
        user_b = max(request.user_a, request.user_b) 
        #get the conversation history from the leader
        for addr in REPLICAS:
            try:
                stub = replica_admin_pb2_grpc.ReplicaAdminStub(grpc.insecure_channel(addr))
                resp = stub.ReadConversation(replica_admin_pb2.ReadConversationRequest(
                    user_a = user_a,
                    user_b = user_b,
                    after_seq=request.after_seq,
                    limit=request.limit, 

                    ), timeout=3 #give up after 3 seconds if no response 
                )
                return direct_gateway_pb2.GetConversationHistoryResponse(events=resp.events, served_by=[addr] ) 
                
            except: 
                continue

        context.set_code(grpc.StatusCode.UNAVAILABLE)
        context.set_details("No replica available")
        return direct_gateway_pb2.GetConversationHistoryResponse()
        #return the conversation history


        
#client sends a message to a server
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) #10 threads
    direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(DirectGatewayServicer(), server) #
    server.add_insecure_port("127.0.0.1:50051") #this is the gateway address 
    server.start() #start the server
    print("Gateway server started on port 50051 running...")
    server.wait_for_termination()
   
if __name__ == '__main__':
    serve()
    