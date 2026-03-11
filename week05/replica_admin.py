"""
Raft replica node. Implements the ReplicaAdmin gRPCservice.
"""

import argparse
import sys
from pathlib import Path
import grpc
from generated import replica_admin_pb2, replica_admin_pb2_grpc
from dataclasses import dataclass
from threading import Lock

@dataclass
class LogEntry:
    term: int
    data: bytes

# Ensure we can import generated stubs

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

class ReplicaAdminServicer(replica_admin_pb2_grpc.ReplicaAdminServicer):
    """Implements the ReplicaAdmin service (Status RPC)."""

    def __init__(self, replica_id: int, host: str, port: int) -> None:
        self.replica_id = replica_id
        self.peer_addrs = self._peer_addrs(host, port)
        self._lock = Lock()

        # Persistent state
        self.current_term = 1
        self.voted_for: int | None = None
        self.log: list[LogEntry] = [LogEntry(term=0, data=b"")] # index 0 is dummy entry

        # Volatile State
        self.commit_index = 0
        self.last_applied = 0
        self.role = replica_admin_pb2.FOLLOWER

        # Leader only (populated when becoming leader)
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

    def _peer_addrs(self, host: str, port: int) -> list[str]:
        base_port = 50061
        addrs = []
        for i in range(1, 6):
            p = base_port + (i - 1)
            if p != port:
                addrs.append(f"{host}:{p}")
        return addrs

    def Status(self, request, context):
        with self._lock:
            last_idx = len(self.log) - 1
            last_term = self.log[-1].term if self.log else 0
            leader_hint = "" # TODO: set leader hint

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