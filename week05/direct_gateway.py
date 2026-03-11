"""
gRPC Gateway for direct messaging. Routes client requests to Raft replicas.
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import grpc
from generated import direct_gateway_pb2, direct_gateway_pb2_grpc
from generated import replica_admin_pb2, replica_admin_pb2_grpc

MAX_SEND_RETRIES = 5
RPC_TIMEOUT = 8.0


class DirectGatewayServicer(direct_gateway_pb2_grpc.DirectGatewayServicer):

    def __init__(self, replica_addrs: list[str]) -> None:
        self.replica_addrs = replica_addrs
        self._cached_leader: str | None = None

    # ── Leader discovery ─────────────────────────────────────────────────────

    def _find_leader(self) -> str | None:
        """Poll Status on each replica and return the leader's address."""
        for addr in self.replica_addrs:
            try:
                channel = grpc.insecure_channel(addr)
                stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                status = stub.Status(replica_admin_pb2.StatusRequest(), timeout=2.0)
                if status.role == replica_admin_pb2.LEADER:
                    self._cached_leader = addr
                    return addr
                if status.leader_hint:
                    self._cached_leader = status.leader_hint
                    return status.leader_hint
            except Exception:
                continue
        return None

    def _get_leader(self) -> str | None:
        """Return cached leader or discover one."""
        if self._cached_leader:
            return self._cached_leader
        return self._find_leader()

    # ── SendDirect ───────────────────────────────────────────────────────────

    def SendDirect(self, request, context):
        leader_addr = self._get_leader()

        for _ in range(MAX_SEND_RETRIES):
            if not leader_addr:
                leader_addr = self._find_leader()
            if not leader_addr:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE,
                    "no leader available",
                )

            try:
                channel = grpc.insecure_channel(leader_addr)
                stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                resp = stub.SubmitMessage(
                    replica_admin_pb2.SubmitMessageRequest(
                        from_user=request.from_user,
                        to_user=request.to_user,
                        client_id=request.client_id,
                        client_msg_id=request.client_msg_id,
                        text=request.text,
                    ),
                    timeout=RPC_TIMEOUT,
                )
                self._cached_leader = leader_addr
                return direct_gateway_pb2.SendDirectResponse(seq=resp.seq)

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    hint = _parse_leader_hint(e.details())
                    self._cached_leader = hint
                    leader_addr = hint
                    continue
                self._cached_leader = None
                leader_addr = None
                continue

        context.abort(grpc.StatusCode.UNAVAILABLE, "failed to reach leader after retries")

    # ── GetConversationHistory ───────────────────────────────────────────────

    def GetConversationHistory(self, request, context):
        ua, ub = sorted([request.user_a, request.user_b])

        target_addr = self._pick_read_target(request)
        if not target_addr:
            context.abort(grpc.StatusCode.UNAVAILABLE, "no replica available for read")

        try:
            channel = grpc.insecure_channel(target_addr)
            stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
            resp = stub.QueryConversation(
                replica_admin_pb2.QueryConversationRequest(
                    user_a=ua,
                    user_b=ub,
                    after_seq=request.after_seq,
                    limit=request.limit,
                ),
                timeout=RPC_TIMEOUT,
            )
        except grpc.RpcError:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"read from {target_addr} failed")

        events = []
        for e in resp.events:
            events.append(direct_gateway_pb2.DirectEvent(
                seq=e.seq,
                from_user=e.from_user,
                text=e.text,
                server_time_ms=e.server_time_ms,
                client_id=e.client_id,
                client_msg_id=e.client_msg_id,
            ))

        return direct_gateway_pb2.GetConversationHistoryResponse(
            events=events,
            served_by=[target_addr],
        )

    def _pick_read_target(self, request) -> str | None:
        pref = request.read_pref

        if pref == direct_gateway_pb2.REPLICA_HINT and request.replica_hint:
            return request.replica_hint

        if pref == direct_gateway_pb2.LEADER_ONLY:
            return self._get_leader()

        # ANY_REPLICA or default: try any replica that responds
        for addr in self.replica_addrs:
            try:
                channel = grpc.insecure_channel(addr)
                stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                stub.Status(replica_admin_pb2.StatusRequest(), timeout=1.0)
                return addr
            except Exception:
                continue
        return None


def _parse_leader_hint(details: str) -> str | None:
    """Extract the hint=host:port from an error details string."""
    if not details:
        return None
    prefix = "hint="
    idx = details.find(prefix)
    if idx < 0:
        return None
    hint = details[idx + len(prefix):]
    hint = hint.strip().rstrip(";").strip()
    return hint if hint else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Direct messaging gateway.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replica-start-port", type=int, default=50061)
    parser.add_argument("--num-replicas", type=int, default=5)
    args = parser.parse_args()

    replica_addrs = [
        f"{args.host}:{args.replica_start_port + i}"
        for i in range(args.num_replicas)
    ]

    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    servicer = DirectGatewayServicer(replica_addrs)
    direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(servicer, server)

    addr = f"{args.host}:{args.port}"
    server.add_insecure_port(addr)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)

    return 0


if __name__ == "__main__":
    sys.exit(main())
