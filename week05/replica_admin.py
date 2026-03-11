"""
Raft replica node. Implements the ReplicaAdmin gRPCservice.
"""

import argparse
from ast import parse
from concurrent.futures import ThreadPoolExecutor
from logging import RootLogger
import sys
from pathlib import Path
import grpc
from generated import replica_admin_pb2, replica_admin_pb2_grpc

# Ensure we can import generated stubs

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

class ReplicaAdminServicer(replica_admin_pb2_grpc.ReplicaAdminServicer):
    """Implements the ReplicaAdmin service (Status RPC)."""

    def __init__(self, replica_id: int, host: str, port: int) -> None:
        self.replica_id = replica_id
        self.peer_addrs = self._peer_addrs(host, port)

    def _peer_addrs(self, host: str, port: int) -> list[str]:
        base_port = 50061
        addrs = []
        for i in range(1, 6):
            p = base_port + (i - 1)
            if p != port:
                addrs.append(f"{host}:{p}")
        return addrs

    def Status(self, request, context):
        return replica_admin_pb2.StatusResponse(
            id=self.replica_id,
            role=replica_admin_pb2.FOLLOWER,
            term=1,
            leader_hint="",
            last_log_index=0,
            last_log_term=0,
            commit_index=0,
        )

def main() -> int:
    parser = argparse.ArgumentParser(description="Raft replica node.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    server = grpc.server(thread_pool=None)
    servicer = ReplicaAdminServicer(
        replica_id=args.port - 50060,
        host=args.host,
        port=args.port,
        )
    replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(servicer, server)

    addr = f"{args.host}:{args.port}"
    server.add_insecure_port(addr)
    server.start()
    server.wait_for_termination()

    return 0

if __name__ == "__main__":
    sys.exit(main())