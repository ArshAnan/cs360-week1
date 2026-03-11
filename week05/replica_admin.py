"""
Raft replica node. Implements the ReplicaAdmin gRPC service.
"""

import argparse
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock

# Ensure generated stubs are importable before any generated imports
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import grpc
from generated import replica_admin_pb2, replica_admin_pb2_grpc
from generated import raft_pb2, raft_pb2_grpc

# ── Tuning constants ────────────────────────────────────────────────────────
ELECTION_TIMEOUT_MIN = 0.15   # seconds (150 ms)
ELECTION_TIMEOUT_MAX = 0.30   # seconds (300 ms)
HEARTBEAT_INTERVAL   = 0.05   # seconds  (50 ms)
MAJORITY             = 3      # out of 5 replicas
RPC_TIMEOUT          = 0.1    # seconds for outgoing Raft RPCs


@dataclass
class LogEntry:
    term: int
    data: bytes


class ReplicaAdminServicer(replica_admin_pb2_grpc.ReplicaAdminServicer):
    """
    Holds all Raft state and implements the ReplicaAdmin.Status RPC.

    Two background daemon threads are started on construction:
    - _election_loop : watches for election timeouts, starts elections
    - _heartbeat_loop: leader sends AppendEntries (heartbeats) to all peers
    """

    def __init__(self, replica_id: int, host: str, port: int) -> None:
        self.replica_id  = replica_id
        self._self_addr  = f"{host}:{port}"
        self._host       = host
        self.peer_addrs  = self._peer_addrs(host, port)
        self._lock       = Lock()
        self._stop_event = Event()

        # ── Persistent Raft state ────────────────────────────────────────────
        self.current_term: int            = 1
        self.voted_for:    int | None     = None
        # log[0] is a dummy sentinel (term=0) so real entries start at index 1
        self.log: list[LogEntry]          = [LogEntry(term=0, data=b"")]

        # ── Volatile state ───────────────────────────────────────────────────
        self.commit_index: int            = 0
        self.last_applied: int            = 0
        self.role:         int            = replica_admin_pb2.FOLLOWER
        self.leader_hint:  str            = ""

        # ── Leader-only state (initialised in _become_leader_locked) ────────
        self.next_index:  dict[int, int]  = {}
        self.match_index: dict[int, int]  = {}

        # ── Election-timer bookkeeping ───────────────────────────────────────
        # _last_heartbeat is reset whenever we receive a valid AE from a leader
        # or grant a vote. If now - _last_heartbeat > _election_timeout we start
        # a new election.
        self._last_heartbeat    = time.monotonic()
        self._election_timeout  = random.uniform(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX
        )

        # Start background threads (daemon so they die with the process)
        threading.Thread(
            target=self._election_loop, daemon=True,
            name=f"election-{replica_id}"
        ).start()
        threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name=f"heartbeat-{replica_id}"
        ).start()

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _peer_addrs(self, host: str, port: int) -> list[str]:
        base = 50061
        return [f"{host}:{base + i}" for i in range(5) if base + i != port]

    def _reset_election_timer(self) -> None:
        """Call under self._lock to prevent a spurious election."""
        self._last_heartbeat   = time.monotonic()
        self._election_timeout = random.uniform(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX
        )

    # ── ReplicaAdmin.Status ──────────────────────────────────────────────────

    def Status(self, request, context):
        with self._lock:
            last_idx  = len(self.log) - 1
            last_term = self.log[-1].term if self.log else 0
            return replica_admin_pb2.StatusResponse(
                id           = self.replica_id,
                role         = self.role,
                term         = self.current_term,
                leader_hint  = self.leader_hint,
                last_log_index = last_idx,
                last_log_term  = last_term,
                commit_index   = self.commit_index,
            )

    # ── Phase 4-A: Election loop (background thread) ─────────────────────────
    #
    # Every 20 ms we check whether the election timer has expired.
    # - If we're the leader, we just reset the timer and continue.
    # - If timeout has passed without a heartbeat, we increment the term,
    #   become a candidate, and call _run_election (outside the lock).
    #
    def _election_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(0.02)  # granularity of our timeout check

            with self._lock:
                if self.role == replica_admin_pb2.LEADER:
                    # Leaders don't need an election timer
                    self._last_heartbeat = time.monotonic()
                    continue

                elapsed = time.monotonic() - self._last_heartbeat
                if elapsed < self._election_timeout:
                    continue  # Timer hasn't expired yet

                # ── Timer expired: become a candidate ────────────────────────
                self.current_term += 1
                self.role          = replica_admin_pb2.CANDIDATE
                self.voted_for     = self.replica_id  # vote for ourselves
                self._reset_election_timer()           # reset for this attempt

                # Snapshot everything needed for the RPC calls
                term_snap   = self.current_term
                last_idx    = len(self.log) - 1
                last_term_v = self.log[-1].term if self.log else 0
                peers       = list(self.peer_addrs)

            # Send RequestVote outside the lock (avoids potential deadlocks)
            self._run_election(term_snap, last_idx, last_term_v, peers)

    # ── Phase 4-B: Run election ───────────────────────────────────────────────
    #
    # Send RequestVote to all peers concurrently.
    # Each response is checked under the lock:
    #   - higher term seen → step down immediately (no longer a candidate)
    #   - vote granted      → count it; become leader if we hit majority
    # After all RPCs finish (or time out), check if we won.
    #
    def _run_election(
        self,
        term:           int,
        last_log_index: int,
        last_log_term:  int,
        peer_addrs:     list[str],
    ) -> None:
        vote_count = [1]      # already have our own vote
        vote_lock  = Lock()

        def request_vote(addr: str) -> None:
            try:
                stub = raft_pb2_grpc.RaftStub(grpc.insecure_channel(addr))
                resp = stub.RequestVote(
                    raft_pb2.RequestVoteRequest(
                        term           = term,
                        candidate_id   = self.replica_id,
                        last_log_index = last_log_index,
                        last_log_term  = last_log_term,
                    ),
                    timeout=RPC_TIMEOUT,
                )
                with self._lock:
                    if resp.term > self.current_term:
                        # Discovered higher term: immediately become follower
                        self.current_term = resp.term
                        self.voted_for    = None
                        self.role         = replica_admin_pb2.FOLLOWER
                        self.leader_hint  = ""
                        return
                if resp.vote_granted:
                    with vote_lock:
                        vote_count[0] += 1
            except Exception:
                pass  # peer unreachable; skip

        threads = [
            threading.Thread(target=request_vote, args=(a,), daemon=True)
            for a in peer_addrs
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=0.15)

        # Re-acquire lock to check whether we won
        with self._lock:
            # Guard: we must still be a candidate in the same term
            if (
                self.role != replica_admin_pb2.CANDIDATE
                or self.current_term != term
            ):
                return
            if vote_count[0] >= MAJORITY:
                self._become_leader_locked()

    # ── Phase 4-C: Transition to leader ──────────────────────────────────────
    #
    # Initialise leader-specific state, then the heartbeat loop will take over.
    #
    def _become_leader_locked(self) -> None:
        """Called while holding self._lock."""
        self.role        = replica_admin_pb2.LEADER
        self.leader_hint = self._self_addr
        log_len = len(self.log)
        for addr in self.peer_addrs:
            peer_id = int(addr.split(":")[-1]) - 50060
            # next_index starts optimistically at the end of our log
            self.next_index[peer_id]  = log_len
            self.match_index[peer_id] = 0

    # ── Phase 5: Heartbeat loop (background thread) ───────────────────────────
    #
    # Every HEARTBEAT_INTERVAL ms, if we are leader, send an empty
    # AppendEntries to every peer.  A response with a higher term causes us
    # to step down to follower.
    #
    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL)

            with self._lock:
                if self.role != replica_admin_pb2.LEADER:
                    continue
                term_snap  = self.current_term
                commit_idx = self.commit_index
                leader_id  = self.replica_id
                last_idx   = len(self.log) - 1
                last_term_v = self.log[-1].term if self.log else 0
                peers      = list(self.peer_addrs)

            def send_heartbeat(addr: str) -> None:
                try:
                    stub = raft_pb2_grpc.RaftStub(grpc.insecure_channel(addr))
                    resp = stub.AppendEntries(
                        raft_pb2.AppendEntriesRequest(
                            term           = term_snap,
                            leader_id      = leader_id,
                            prev_log_index = last_idx,
                            prev_log_term  = last_term_v,
                            entries        = [],
                            leader_commit  = commit_idx,
                        ),
                        timeout=RPC_TIMEOUT,
                    )
                    with self._lock:
                        if resp.term > self.current_term:
                            self.current_term = resp.term
                            self.voted_for    = None
                            self.role         = replica_admin_pb2.FOLLOWER
                            self.leader_hint  = ""
                except Exception:
                    pass  # peer unreachable; skip

            for addr in peers:
                threading.Thread(
                    target=send_heartbeat, args=(addr,), daemon=True
                ).start()

    def stop(self) -> None:
        """Signal background threads to exit."""
        self._stop_event.set()


# ── RaftServicer ──────────────────────────────────────────────────────────────
#
# Handles incoming Raft RPCs from peer replicas.
# All state lives in the ReplicaAdminServicer; this class just borrows a
# reference to it via self.admin.
#

class RaftServicer(raft_pb2_grpc.RaftServicer):
    """Handles RequestVote and AppendEntries from other replicas."""

    def __init__(self, admin_servicer: ReplicaAdminServicer) -> None:
        self.admin = admin_servicer

    # ── RequestVote ───────────────────────────────────────────────────────────
    #
    # Rules:
    # 1. Reject if request.term < our term.
    # 2. If request.term > our term, update term, clear voted_for, become follower.
    # 3. Grant vote only if we haven't voted yet (or already for this candidate)
    #    AND the candidate's log is at least as up-to-date as ours
    #    (compare last_log_term first, then last_log_index).
    # 4. Granting a vote resets our election timer.
    #
    def RequestVote(self, request, context):
        with self.admin._lock:
            if request.term < self.admin.current_term:
                return raft_pb2.RequestVoteResponse(
                    term=self.admin.current_term, vote_granted=False
                )

            if request.term > self.admin.current_term:
                self.admin.current_term = request.term
                self.admin.voted_for    = None
                self.admin.role         = replica_admin_pb2.FOLLOWER
                self.admin.leader_hint  = ""

            can_vote = (
                self.admin.voted_for is None
                or self.admin.voted_for == request.candidate_id
            )
            if can_vote:
                last_idx  = len(self.admin.log) - 1
                last_term = self.admin.log[-1].term if self.admin.log else 0
                up_to_date = (
                    request.last_log_term > last_term
                    or (
                        request.last_log_term == last_term
                        and request.last_log_index >= last_idx
                    )
                )
                if up_to_date:
                    self.admin.voted_for = request.candidate_id
                    self.admin._reset_election_timer()  # don't start our own election
                    return raft_pb2.RequestVoteResponse(
                        term=self.admin.current_term, vote_granted=True
                    )

            return raft_pb2.RequestVoteResponse(
                term=self.admin.current_term, vote_granted=False
            )

    # ── AppendEntries ─────────────────────────────────────────────────────────
    #
    # Rules (also doubles as the heartbeat receiver):
    # 1. Reject if request.term < our term.
    # 2. If request.term >= our term: update term if higher, become follower,
    #    reset election timer, update leader_hint.
    # 3. Reject (success=False) if prev_log_index is out of bounds or term mismatch.
    # 4. Append/overwrite entries from prev_log_index+1 onward.
    # 5. Advance commit_index to min(leader_commit, last new entry index).
    #
    def AppendEntries(self, request, context):
        with self.admin._lock:
            if request.term < self.admin.current_term:
                return raft_pb2.AppendEntriesResponse(
                    term=self.admin.current_term, success=False
                )

            # Step down / sync term
            if request.term > self.admin.current_term:
                self.admin.current_term = request.term
                self.admin.voted_for    = None

            # Valid AE from current leader
            self.admin.role        = replica_admin_pb2.FOLLOWER
            # Derive leader address from leader_id (port = 50060 + leader_id)
            self.admin.leader_hint = f"{self.admin._host}:{50060 + request.leader_id}"
            self.admin._reset_election_timer()

            prev_idx = request.prev_log_index

            # Consistency check: prev must exist in our log
            if prev_idx >= len(self.admin.log):
                return raft_pb2.AppendEntriesResponse(
                    term=self.admin.current_term, success=False
                )
            if prev_idx >= 0 and self.admin.log[prev_idx].term != request.prev_log_term:
                # Term conflict: truncate diverging suffix; leader will retry
                self.admin.log = self.admin.log[:prev_idx]
                return raft_pb2.AppendEntriesResponse(
                    term=self.admin.current_term, success=False
                )

            # Append entries (skip already-matching ones, overwrite conflicts)
            for i, ent in enumerate(request.entries):
                idx   = prev_idx + 1 + i
                entry = LogEntry(term=ent.term, data=bytes(ent.data))
                if idx < len(self.admin.log):
                    if self.admin.log[idx].term != ent.term:
                        # Conflict: truncate from here and append new entry
                        self.admin.log = self.admin.log[:idx]
                        self.admin.log.append(entry)
                    # else: entry already matches, skip
                else:
                    self.admin.log.append(entry)

            # Advance commit index
            if request.leader_commit > self.admin.commit_index:
                self.admin.commit_index = min(
                    request.leader_commit,
                    len(self.admin.log) - 1,
                )

            return raft_pb2.AppendEntriesResponse(
                term=self.admin.current_term, success=True
            )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Raft replica node.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    server = grpc.server(ThreadPoolExecutor(max_workers=10))

    admin_servicer = ReplicaAdminServicer(
        replica_id = args.port - 50060,
        host       = args.host,
        port       = args.port,
    )
    raft_servicer = RaftServicer(admin_servicer)

    replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(admin_servicer, server)
    raft_pb2_grpc.add_RaftServicer_to_server(raft_servicer, server)

    addr = f"{args.host}:{args.port}"
    server.add_insecure_port(addr)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        admin_servicer.stop()
        server.stop(grace=1)

    return 0


if __name__ == "__main__":
    sys.exit(main())
