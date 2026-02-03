#!/usr/bin/env python3
"""
primary_node.py

Primary coordinator that:
1) Maintains an in-memory registry of secondary nodes (registered by secondary_node.py)
2) Distributes prime-range computation requests to registered secondary nodes
3) Aggregates results in memory and returns a final result (count or list sample)

Endpoints
---------
GET  /health
GET  /nodes
POST /register
POST /compute
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import random  # CHANGE: used for jitter in retry backoff
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

# CHANGE: Local fallback computation when no secondaries are available
from primes_in_range import get_primes


class Registry:
    def __init__(self, ttl_s: int = 3600):
        self.ttl_s = ttl_s
        self.lock = threading.Lock()
        self.nodes: Dict[str, Dict[str, Any]] = {}

    def upsert(self, node: Dict[str, Any]) -> Dict[str, Any]:
        node_id = str(node["node_id"])
        now = time.time()
        record = {
            "node_id": node_id,
            "host": str(node["host"]),
            "port": int(node["port"]),
            "cpu_count": int(node.get("cpu_count", 1)),
            "last_seen": float(node.get("ts", now)),
            "registered_at": now,
        }
        with self.lock:
            if node_id in self.nodes:
                record["registered_at"] = self.nodes[node_id].get("registered_at", now)
            self.nodes[node_id] = record
            return record

    def active_nodes(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self.lock:
            stale = [nid for nid, rec in self.nodes.items() if (now - float(rec.get("last_seen", 0))) > self.ttl_s]
            for nid in stale:
                del self.nodes[nid]
            return list(self.nodes.values())


REGISTRY = Registry(ttl_s=120)

# CHANGE: Track secondary node health to gracefully handle partial failures.
# Implements a simple circuit breaker (Chapter 8 concept): after repeated failures,
# temporarily stop sending work to a node and retry slices elsewhere.
_NODE_HEALTH_LOCK = threading.Lock()
NODE_HEALTH: Dict[str, Dict[str, Any]] = {}  # node_id -> {"fail_count": int, "down_until": float}

# Tunables (kept conservative for a class prototype)
FAILURE_THRESHOLD = 2       # failures before we "trip" the circuit
COOLDOWN_SECONDS = 20.0     # how long to avoid a failing node
DEFAULT_NODE_TIMEOUT_S = 12 # per-request timeout to a secondary node
MAX_SLICE_ATTEMPTS = 4      # retry a slice this many times before local fallback



def _post_json(url: str, payload: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def split_into_slices(low: int, high: int, n: int) -> List[Tuple[int, int]]:
    if n <= 0:
        return []
    total = high - low
    base = total // n
    rem = total % n
    out = []
    start = low
    for i in range(n):
        size = base + (1 if i < rem else 0)
        end = start + size
        if start < end:
            out.append((start, end))
        start = end
    return out


def distributed_compute(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    CHANGE: Failure-tolerant distributed compute.

    Concepts used from Petrov Ch.8 (Distributed Computing overview):
    - Partial failures: one worker can crash without failing the whole request.
    - Timeouts + retries: detect suspected failure via timeout/connection errors and retry elsewhere.
    - Circuit breaker + backoff/jitter: avoid hammering unhealthy nodes and causing cascading failures.
    """
    low = int(payload["low"])
    high = int(payload["high"])
    if high <= low:
        raise ValueError("high must be > low")

    mode = str(payload.get("mode", "count"))
    if mode not in ("count", "list"):
        raise ValueError("mode must be 'count' or 'list'")

    sec_exec = str(payload.get("secondary_exec", "processes"))
    if sec_exec not in ("single", "threads", "processes"):
        raise ValueError("secondary_exec must be single|threads|processes")

    sec_workers = payload.get("secondary_workers", None)
    if sec_workers is not None:
        sec_workers = int(sec_workers)

    max_return_primes = int(payload.get("max_return_primes", 5000))
    include_per_node = bool(payload.get("include_per_node", False))

    chunk = int(payload.get("chunk", 500_000))

    # CHANGE: timeout can be overridden by the client, but has a safe default.
    node_timeout_s = int(payload.get("node_timeout_s", DEFAULT_NODE_TIMEOUT_S))

    # Helper: circuit breaker checks
    def _is_node_available(node_id: str) -> bool:
        with _NODE_HEALTH_LOCK:
            st = NODE_HEALTH.get(node_id)
            if not st:
                return True
            return time.time() >= float(st.get("down_until", 0.0))

    def _record_success(node_id: str) -> None:
        with _NODE_HEALTH_LOCK:
            NODE_HEALTH[node_id] = {"fail_count": 0, "down_until": 0.0}

    def _record_failure(node_id: str) -> None:
        with _NODE_HEALTH_LOCK:
            st = NODE_HEALTH.get(node_id, {"fail_count": 0, "down_until": 0.0})
            st["fail_count"] = int(st.get("fail_count", 0)) + 1
            if st["fail_count"] >= FAILURE_THRESHOLD:
                st["down_until"] = time.time() + COOLDOWN_SECONDS
            NODE_HEALTH[node_id] = st

    # Helper: local fallback. (Allowed by assignment: "gracefully handle failure".)
    def _local_compute(sl_low: int, sl_high: int) -> Dict[str, Any]:
        t0_local = time.perf_counter()
        if mode == "count":
            cnt = int(get_primes(sl_low, sl_high, return_list=False))
            primes_list: List[int] | None = None
            max_p = -1  # we do not compute max prime in count-only mode
        else:
            # CHANGE: for list mode we still need the *true* count; compute full list once,
            # then truncate only the returned sample.
            primes_full = list(get_primes(sl_low, sl_high, return_list=True))
            cnt = len(primes_full)
            max_p = primes_full[-1] if primes_full else -1
            primes_list = primes_full[:max_return_primes]
        t1_local = time.perf_counter()
        return {
            "node_id": "primary",
            "node": {"host": "localhost", "port": -1, "cpu_count": 0},
            "slice": [sl_low, sl_high],
            "round_trip_s": 0.0,
            "node_elapsed_s": t1_local - t0_local,
            "node_sum_chunk_s": t1_local - t0_local,
            "total_primes": cnt,
            "max_prime": max_p,
            "primes": primes_list,
            "primes_truncated": (mode == "list" and int(cnt) > max_return_primes),  # CHANGE: true truncation flag
            "fallback": True,
        }

    # CHANGE: choose number of slices based on current membership.
    nodes = REGISTRY.active_nodes()
    if not nodes:
        # If no secondaries exist at all, compute locally.
        slices = [(low, high)]
    else:
        nodes_sorted = sorted(nodes, key=lambda n: n["node_id"])
        slices = split_into_slices(low, high, len(nodes_sorted))

    t0 = time.perf_counter()

    # Slice attempt tracking: avoids infinite loops if everything is down.
    attempts: Dict[Tuple[int, int], int] = {tuple(sl): 0 for sl in slices}
    pending: List[Tuple[int, int]] = [tuple(sl) for sl in slices]
    completed: List[Dict[str, Any]] = []

    def _call_node(node: Dict[str, Any], sl: Tuple[int, int]) -> Dict[str, Any]:
        """Call a secondary node for a slice; raises on failure."""
        host = node["host"]
        port = node["port"]
        url = f"http://{host}:{port}/compute"
        req = {
            "low": sl[0],
            "high": sl[1],
            "mode": mode,
            "chunk": chunk,
            "exec": sec_exec,
            "workers": sec_workers,
            "max_return_primes": max_return_primes if mode == "list" else 0,
            "include_per_chunk": False,
        }
        req = {k: v for k, v in req.items() if v is not None}

        t_call0 = time.perf_counter()
        resp = _post_json(url, req, timeout_s=node_timeout_s)
        t_call1 = time.perf_counter()

        if not resp.get("ok"):
            raise RuntimeError(f"node {node['node_id']} error: {resp}")

        node_elapsed_s = float(resp.get("elapsed_seconds", 0.0))
        # CHANGE: fixed quotes above.
        print(f"Node ID: {node['node_id']} completed in: {node_elapsed_s}")

        return {
            "node_id": node["node_id"],
            "node": {"host": host, "port": port, "cpu_count": node.get("cpu_count", 1)},
            "slice": list(sl),
            "round_trip_s": t_call1 - t_call0,
            "node_elapsed_s": node_elapsed_s,
            "node_sum_chunk_s": float(resp.get("sum_chunk_compute_seconds", 0.0)),
            "total_primes": int(resp.get("total_primes", 0)),
            "max_prime": int(resp.get("max_prime", -1)),
            "primes": resp.get("primes", None),
            "primes_truncated": bool(resp.get("primes_truncated", False)),
        }

    # CHANGE: main scheduler loop: keep assigning pending slices to healthy nodes.
    while pending:
        # Refresh membership and filter by circuit breaker (and registry TTL).
        nodes_now = sorted(REGISTRY.active_nodes(), key=lambda n: n["node_id"])
        healthy_nodes = [n for n in nodes_now if _is_node_available(str(n["node_id"]))]

        if not healthy_nodes:
            # No secondaries available -> local fallback for remaining slices.
            for sl in pending:
                completed.append(_local_compute(sl[0], sl[1]))
            pending.clear()
            break

        # Assign up to one slice per available node for this "round".
        round_pairs = list(zip(healthy_nodes, pending[: len(healthy_nodes)]))
        pending = pending[len(round_pairs) :]

        # Fire off calls concurrently.
        with ThreadPoolExecutor(max_workers=min(32, len(round_pairs))) as ex:
            fut_map = {ex.submit(_call_node, node, sl): (node, sl) for node, sl in round_pairs}

            for fut in as_completed(fut_map):
                node, sl = fut_map[fut]
                node_id = str(node["node_id"])

                try:
                    r = fut.result()
                    _record_success(node_id)
                    completed.append(r)
                except Exception as e:
                    # CHANGE: Handle partial failure without crashing the whole job.
                    _record_failure(node_id)

                    # Track attempts per slice; after too many tries, fallback locally.
                    attempts[sl] = attempts.get(sl, 0) + 1
                    if attempts[sl] >= MAX_SLICE_ATTEMPTS:
                        completed.append(_local_compute(sl[0], sl[1]))
                    else:
                        # Backoff + jitter to reduce retry storms/cascading failures.
                        backoff = min(2.0, 0.25 * (2 ** (attempts[sl] - 1)))
                        time.sleep(backoff + random.random() * 0.15)
                        pending.append(sl)

                    # Optional: include a failure record for debugging when requested.
                    if include_per_node:
                        completed.append({
                            "node_id": node_id,
                            "node": {"host": node["host"], "port": node["port"], "cpu_count": node.get("cpu_count", 1)},
                            "slice": list(sl),
                            "error": str(e),
                            "ok": False,
                        })

    # Aggregate results (ignore failure-record-only entries that don't have totals).
    completed.sort(key=lambda r: r.get("slice", [0])[0])

    total_primes = 0
    primes_sample: List[int] = []
    primes_truncated = False
    max_prime = -1

    per_node_results: List[Dict[str, Any]] = []
    for r in completed:
        # skip failure-only debug entries
        if "total_primes" not in r:
            per_node_results.append(r)
            continue

        per_node_results.append(r)
        total_primes += int(r["total_primes"])
        max_prime = max(max_prime, int(r.get("max_prime", -1)))

        if mode == "list" and r.get("primes") is not None:
            ps = list(r["primes"])
            if len(primes_sample) < max_return_primes:
                remaining = max_return_primes - len(primes_sample)
                primes_sample.extend(ps[:remaining])
                if len(ps) > remaining:
                    primes_truncated = True
            else:
                primes_truncated = True
            if r.get("primes_truncated"):
                primes_truncated = True

    t1 = time.perf_counter()

    resp: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range": [low, high],
        "nodes_used": len([r for r in per_node_results if r.get("node_id") not in (None, "primary") and r.get("total_primes") is not None]),
        "secondary_exec": sec_exec,
        "secondary_workers": sec_workers,
        "chunk": chunk,
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_node_compute_seconds": sum(float(r.get("node_elapsed_s", 0.0)) for r in per_node_results if r.get("total_primes") is not None),
        "sum_node_round_trip_seconds": sum(float(r.get("round_trip_s", 0.0)) for r in per_node_results if r.get("total_primes") is not None),
        "node_timeout_s": node_timeout_s,  # CHANGE: visible in response for debugging
    }

    if mode == "list":
        resp["primes"] = primes_sample
        resp["primes_truncated"] = primes_truncated
        resp["max_return_primes"] = max_return_primes

    if include_per_node:
        resp["per_node"] = per_node_results

    return resp



class Handler(BaseHTTPRequestHandler):
    server_version = "PrimaryPrimeCoordinator/1.0"

    def _send_json(self, obj: Dict[str, Any], code: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            return self._send_json({"ok": True, "status": "healthy"})
        if parsed.path == "/nodes":
            nodes = REGISTRY.active_nodes()
            nodes.sort(key=lambda n: n["node_id"])
            return self._send_json({"ok": True, "nodes": nodes, "ttl_s": REGISTRY.ttl_s})
        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            return self._send_json({"ok": False, "error": "invalid content-length"}, code=400)

        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except Exception as e:
            return self._send_json({"ok": False, "error": f"bad json: {e}"}, code=400)

        if parsed.path == "/register":
            for k in ("node_id", "host", "port"):
                if k not in payload:
                    return self._send_json({"ok": False, "error": f"missing field: {k}"}, code=400)
            rec = REGISTRY.upsert(payload)
            print(f"[primary_node] Added node: {payload} to registry")
            return self._send_json({"ok": True, "node": rec})

        if parsed.path == "/compute":
            try:
                for k in ("low", "high"):
                    if k not in payload:
                        raise ValueError(f"missing field: {k}")
                resp = distributed_compute(payload)
                return self._send_json(resp, code=200)
            except Exception as e:
                return self._send_json({"ok": False, "error": str(e)}, code=400)

        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def log_message(self, fmt, *args):
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive (default 3600).")
    args = ap.parse_args()

    global REGISTRY
    REGISTRY = Registry(ttl_s=max(10, int(args.ttl)))

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[primary_node] listening on http://{args.host}:{args.port}")
    print("  GET  /health")
    print("  GET  /nodes")
    print("  POST /register")
    print("  POST /compute")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
    finally:
        httpd.server_close()
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()
