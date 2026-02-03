import requests as r
from pprint import pprint

primary_node_url = "http://127.0.0.1:9200"
print("-----Terminal to Interact with Secondary Nodes-----")
print("Supported Commands: h - health, i - info, c - compute")
print()
command = ""
while True:
    # to interact with live secondary nodes, get all nodes from the server
    # add try block later...
    response = r.get(f"{primary_node_url}/nodes").json()
    all_nodes = {node.get("node_id") : {"url": 
                                        f"http://{node.get('host')}:{node.get('port')}"} for node in response.get("nodes")}

    # communication channel definition
    print("___ ALL Nodes ___")
    print(all_nodes)
    print()

    node_id = input("Enter a node id: ")

    command = input("Enter a command: ")
    if command == "q":
         break
    secondary_node = all_nodes.get(node_id)
    secondary_node_url = secondary_node.get("url")
    match command.lower():
        case "h":
            response = r.get(f"{secondary_node_url}/health").json()
            
        case "i":
            response = r.get(f"{secondary_node_url}/info").json()

        case "c":
            response = {"status": "Not Implemented Yet."}
         
    if response:
            pprint(response, indent=2)
            print()
    else:
         print("A network error occured")
         print()



