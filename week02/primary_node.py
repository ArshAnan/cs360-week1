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
from platform import node
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import grpc
from grpc_status import rpc_status
from google.protobuf import any_pb2
from google.rpc import status_pb2, error_details_pb2
import primes_pb2
import primes_pb2_grpc


class Registry:
    def __init__(self, ttl_s: int = 3600):
        self.ttl_s = ttl_s
        self.lock = threading.Lock()
        self.nodes: Dict[str, Dict[str, Any]] = {}

    def upsert(self, node: Dict[str, Any]) -> Dict[str, Any]:
        node_id = str(node["node_id"])
        now = time.time()
        port = int(node["port"])
        record = {
            "node_id": node_id,
            "host": str(node["host"]),
            "port": port,
            "http_port": int(node.get("http_port", port)),  # defaults to port if not provided
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


# ---------------------------------------------------------------------------
# Helper: build a rich gRPC error from a downstream RPC or local exception
# ---------------------------------------------------------------------------

def _abort_with_status(context, code: grpc.StatusCode, message: str,
                       debug_info: str | None = None,
                       bad_field: str | None = None) -> None:
    """
    Set a structured gRPC error on *context* using google.rpc rich error model.
    - debug_info: optional traceback / extra detail for DebugInfo
    - bad_field: if set, adds a BadRequest field violation
    """
    detail_protos: list = []

    if debug_info:
        dbg = error_details_pb2.DebugInfo(
            stack_entries=debug_info.strip().splitlines(),
            detail=message,
        )
        any_dbg = any_pb2.Any()
        any_dbg.Pack(dbg)
        detail_protos.append(any_dbg)

    if bad_field:
        br = error_details_pb2.BadRequest()
        fv = br.field_violations.add()
        fv.field = bad_field
        fv.description = message
        any_br = any_pb2.Any()
        any_br.Pack(br)
        detail_protos.append(any_br)

    rich_status = status_pb2.Status(
        code=code.value[0],
        message=message,
        details=detail_protos,
    )
    context.abort_with_status(rpc_status.to_status(rich_status))


def _grpc_error_to_detail_string(rpc_error: grpc.RpcError) -> str:
    """Extract useful info from a downstream grpc.RpcError."""
    parts = [f"code={rpc_error.code()}", f"details={rpc_error.details()}"]
    # Try to unpack rich status from the downstream error
    try:
        status = rpc_status.from_call(rpc_error)
        if status:
            for detail in status.details:
                parts.append(f"rich_detail={detail}")
    except Exception:
        pass
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# gRPC CoordinatorService implementation
# ---------------------------------------------------------------------------

class CoordinatorServiceImpl(primes_pb2_grpc.CoordinatorServiceServicer):
    """
    gRPC implementation of CoordinatorService.
    Runs side-by-side with the existing HTTP endpoints.
    """

    # ---- RegisterNode ----
    def RegisterNode(self, request, context):
        # Extract HTTP port from gRPC metadata if the worker sent it
        metadata = dict(context.invocation_metadata())
        http_port_str = metadata.get('http-port', None)

        node_dict = {
            "node_id": request.node_id,
            "host": request.host,
            "port": request.port,
            "cpu_count": request.cpu_count,
            "ts": request.ts,
        }
        if http_port_str is not None:
            node_dict["http_port"] = int(http_port_str)

        rec = REGISTRY.upsert(node_dict)
        print(f"[primary_node][gRPC] registered node: {request.node_id} "
              f"(grpc_port={request.port}, http_port={rec.get('http_port')})")

        node_msg = primes_pb2.Node(
            node_id=rec["node_id"],
            host=rec["host"],
            port=rec["port"],
            cpu_count=rec["cpu_count"],
            last_seen=rec["last_seen"],
            registered_at=rec["registered_at"],
        )
        return primes_pb2.RegisterNodeResponse(node=node_msg)

    # ---- ListNodes ----
    def ListNodes(self, request, context):
        nodes = REGISTRY.active_nodes()
        nodes.sort(key=lambda n: n["node_id"])

        node_msgs = []
        for n in nodes:
            node_msgs.append(primes_pb2.Node(
                node_id=n["node_id"],
                host=n["host"],
                port=int(n["port"]),
                cpu_count=int(n.get("cpu_count", 1)),
                last_seen=float(n.get("last_seen", 0)),
                registered_at=float(n.get("registered_at", 0)),
            ))
        return primes_pb2.ListNodesResponse(nodes=node_msgs, ttl_s=REGISTRY.ttl_s)

    # ---- Compute ----
    def Compute(self, request, context):
        """
        Split [low, high) across registered worker nodes, fan out
        ComputeRange RPCs in parallel, aggregate results, enforce
        deadlines, and convert downstream errors to structured gRPC
        status/details.
        """
        low = request.low
        high = request.high

        # --- validate inputs ---
        if high <= low:
            _abort_with_status(context, grpc.StatusCode.INVALID_ARGUMENT,
                               "high must be > low", bad_field="high")
            return primes_pb2.ComputeResponse()

        # Map protobuf enums → string equivalents (for logging / compatibility)
        mode_enum = request.mode
        if mode_enum == primes_pb2.MODE_LIST:
            mode = "list"
        else:
            mode = "count"

        exec_enum = request.secondary_exec
        if exec_enum == primes_pb2.EXEC_THREADS:
            sec_exec = "threads"
        elif exec_enum == primes_pb2.EXEC_PROCESSES:
            sec_exec = "processes"
        else:
            sec_exec = "single"

        sec_workers = request.secondary_workers  # 0 means "use worker default"
        chunk = request.chunk if request.chunk > 0 else 500_000
        max_return_primes = request.max_return_primes if request.max_return_primes > 0 else 5000
        include_per_node = request.include_per_node

        # --- discover active worker nodes ---
        nodes = REGISTRY.active_nodes()
        if not nodes:
            _abort_with_status(context, grpc.StatusCode.FAILED_PRECONDITION,
                               "no active secondary nodes registered")
            return primes_pb2.ComputeResponse()

        nodes_sorted = sorted(nodes, key=lambda n: n["node_id"])
        slices = split_into_slices(low, high, len(nodes_sorted))
        nodes_sorted = nodes_sorted[:len(slices)]

        t0 = time.perf_counter()

        # --- build per-node RPC call ---
        WORKER_TIMEOUT_S = 120  # deadline for each downstream ComputeRange RPC

        def call_worker(node_rec: Dict[str, Any], sl: Tuple[int, int]) -> Dict[str, Any]:
            """
            Open a channel to the worker, send ComputeRange, and return a
            result dict compatible with the existing aggregation logic.
            """
            host = node_rec["host"]
            port = int(node_rec["port"])
            address = f"{host}:{port}"

            # Build ComputeRangeRequest
            cr_request = primes_pb2.ComputeRangeRequest(
                low=sl[0],
                high=sl[1],
                mode=mode_enum,
                chunk=chunk,
                exec_mode=exec_enum,
                workers=sec_workers,
                max_return_primes=max_return_primes if mode == "list" else 0,
                include_per_chunk=False,
            )

            t_call0 = time.perf_counter()
            channel = grpc.insecure_channel(address)
            stub = primes_pb2_grpc.WorkerServiceStub(channel)

            max_retries = 3
            resp = None
            for attempt in range(max_retries):
                try:
                    print(f"[primary_node][gRPC] Attempt {attempt + 1}: calling worker {node_rec['node_id']} at {address} ...")
                    resp = stub.ComputeRange(cr_request, timeout=WORKER_TIMEOUT_S)
                    if resp.ok:
                        print(f"[primary_node][gRPC] SUCCESS! worker {node_rec['node_id']} returned data")
                        break
                    else:
                        raise RuntimeError(f"worker {node_rec['node_id']} returned ok=false: {resp.error}")
                except grpc.RpcError as rpc_err:
                    detail = _grpc_error_to_detail_string(rpc_err)
                    if attempt < max_retries - 1:
                        print(f"[primary_node][gRPC] Attempt {attempt + 1} failed for {node_rec['node_id']}: {detail}. "
                              f"Retrying in {2 ** attempt}s ...")
                        time.sleep(2 ** attempt)
                    else:
                        # Re-raise on final attempt so the outer handler can
                        # convert it into a structured error.
                        channel.close()
                        raise
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"[primary_node][gRPC] Attempt {attempt + 1} failed for {node_rec['node_id']}: {e}. "
                              f"Retrying in {2 ** attempt}s ...")
                        time.sleep(2 ** attempt)
                    else:
                        channel.close()
                        raise

            t_call1 = time.perf_counter()
            channel.close()

            node_elapsed_s = float(resp.elapsed_seconds) if resp else 0.0
            print(f"[primary_node][gRPC] worker {node_rec['node_id']} completed in {node_elapsed_s:.3f}s")

            return {
                "node_id": node_rec["node_id"],
                "host": host,
                "port": port,
                "cpu_count": node_rec.get("cpu_count", 1),
                "slice": sl,
                "round_trip_s": t_call1 - t_call0,
                "node_elapsed_s": node_elapsed_s,
                "node_sum_chunk_s": float(resp.sum_chunk_compute_seconds) if resp else 0.0,
                "total_primes": int(resp.total_primes) if resp else 0,
                "max_prime": int(resp.max_prime) if resp else -1,
                "primes": list(resp.primes) if resp and mode == "list" else None,
                "primes_truncated": bool(resp.primes_truncated) if resp else False,
            }

        # --- fan out ComputeRange calls in parallel ---
        per_node_results: List[Dict[str, Any]] = []
        errors: List[str] = []

        with ThreadPoolExecutor(max_workers=min(32, len(nodes_sorted))) as ex:
            futs = {
                ex.submit(call_worker, nd, sl): nd
                for nd, sl in zip(nodes_sorted, slices)
            }
            for f in as_completed(futs):
                nd = futs[f]
                try:
                    per_node_results.append(f.result())
                except grpc.RpcError as rpc_err:
                    detail = _grpc_error_to_detail_string(rpc_err)
                    errors.append(f"worker {nd['node_id']}: {detail}")
                except Exception as exc:
                    errors.append(f"worker {nd['node_id']}: {exc}")

        # If all workers failed, abort with structured error
        if errors and not per_node_results:
            _abort_with_status(
                context,
                grpc.StatusCode.UNAVAILABLE,
                f"all worker RPCs failed: {'; '.join(errors)}",
                debug_info="\n".join(errors),
            )
            return primes_pb2.ComputeResponse()

        # Warn (but continue) if some workers failed
        if errors:
            print(f"[primary_node][gRPC] WARNING: some workers failed: {errors}")

        # --- aggregate results (mirrors distributed_compute logic exactly) ---
        per_node_results.sort(key=lambda r: r["slice"][0])

        total_primes = 0
        max_prime = -1
        primes_sample: List[int] = []
        primes_truncated = False

        for r in per_node_results:
            total_primes += int(r["total_primes"])
            max_prime = max(max_prime, int(r["max_prime"]))
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

        # --- build ComputeResponse ---
        response = primes_pb2.ComputeResponse(
            ok=True,
            mode=mode_enum,
            range_low=low,
            range_high=high,
            nodes_used=len(nodes_sorted),
            secondary_exec=exec_enum,
            secondary_workers=sec_workers,
            chunk=chunk,
            total_primes=total_primes,
            max_prime=max_prime,
            elapsed_seconds=t1 - t0,
            sum_node_compute_seconds=sum(float(r["node_elapsed_s"]) for r in per_node_results),
            sum_node_round_trip_seconds=sum(float(r["round_trip_s"]) for r in per_node_results),
        )

        if mode == "list":
            response.primes.extend(primes_sample)
            response.primes_truncated = primes_truncated
            response.max_return_primes = max_return_primes

        if include_per_node:
            for r in per_node_results:
                pnr = response.per_node.add()
                pnr.node_id = r["node_id"]
                pnr.slice_low = r["slice"][0]
                pnr.slice_high = r["slice"][1]
                pnr.round_trip_s = r["round_trip_s"]
                pnr.node_elapsed_s = r["node_elapsed_s"]
                pnr.node_sum_chunk_s = r["node_sum_chunk_s"]
                pnr.total_primes = r["total_primes"]
                pnr.max_prime = r["max_prime"]
                if r.get("primes") is not None:
                    pnr.primes.extend(r["primes"])
                    pnr.primes_truncated = r.get("primes_truncated", False)

        return response


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

    nodes = REGISTRY.active_nodes()
    if not nodes:
        raise ValueError("no active secondary nodes registered")
    
    chunk = int(payload.get("chunk", 500_000))

    nodes_sorted = sorted(nodes, key=lambda n: n["node_id"])
    slices = split_into_slices(low, high, len(nodes_sorted))
    nodes_sorted = nodes_sorted[:len(slices)]

    t0 = time.perf_counter()

    per_node_results: List[Dict[str, Any]] = []
    total_primes = 0
    primes_sample: List[int] = []
    primes_truncated = False
    max_prime = -1

    def call_node(node: Dict[str, Any], sl: Tuple[int, int]) -> Dict[str, Any]:
        host = node["host"]
        port = node.get("http_port", node["port"])  # prefer HTTP port for HTTP calls
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
        max_retries = 3
        for attempt in range(max_retries): #retry 3 time before giving up
            try:
                        
                print(f"Attempt {attempt + 1}: Calling {node['node_id']} ...") #retry attempt log
                t_call0 = time.perf_counter() #start timer
                resp = _post_json(url, req, timeout_s=120) #reduced timeout for faster failure response
                t_call1 = time.perf_counter() #end timer
                if not resp.get("ok"):
                    raise RuntimeError(f"node {node['node_id']} error: {resp}")
                if resp.get("ok") == True: #check if response is ok
                    print(f"SUCCESS! {node['node_id']} returned data!")
                    break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed for {node['node_id']}: {e}. Retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)  # time delay before next retry
                else:
                    print(f"FAILED! {node['node_id']} did not respond after {max_retries} attempts... Will now be switching to a working node.")
                   
                    working_nodes = []
                    for n in nodes_sorted:
                        if n["node_id"] != node["node_id"]: # checks that we don't retry the same failed node
                            working_nodes.append(n) # add the working node to the list

                    if working_nodes:
                        new_node = working_nodes[0] # pick the first working node
                        print(f"Switching to working node: {new_node['node_id']}")
                        return call_node(new_node, sl) # recursive call to the working node
                    else:
                        print(f"FAILED! {node['node_id']} did not respond after {max_retries} attempts")
                        return None

        
        node_elapsed_s = float(resp.get("elapsed_seconds", 0.0))
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

    with ThreadPoolExecutor(max_workers=min(32, len(nodes_sorted))) as ex:
        futs = [ex.submit(call_node, node, sl) for node, sl in zip(nodes_sorted, slices)]
        for f in as_completed(futs):
            result = f.result()
            if result is not None:
                per_node_results.append(result)

    if not per_node_results:
        raise ValueError("all worker nodes failed to respond")

    per_node_results.sort(key=lambda r: r["slice"][0])

    for r in per_node_results:
        total_primes += int(r["total_primes"])
        max_prime = max(max_prime, int(r["max_prime"]))
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
        "nodes_used": len(nodes_sorted),
        "secondary_exec": sec_exec,
        "secondary_workers": sec_workers,
        "chunk": chunk,
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_node_compute_seconds": sum(float(r["node_elapsed_s"]) for r in per_node_results),
        "sum_node_round_trip_seconds": sum(float(r["round_trip_s"]) for r in per_node_results),
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
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation (HTTP + gRPC).")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9200, help="HTTP bind port (default 9200).")
    ap.add_argument("--grpc-port", type=int, default=50050, help="gRPC bind port (default 50050).")
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive (default 3600).")
    args = ap.parse_args()

    global REGISTRY
    REGISTRY = Registry(ttl_s=max(10, int(args.ttl)))

    # Start gRPC server in background thread
    grpc_server = grpc.server(ThreadPoolExecutor(max_workers=10))
    primes_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        CoordinatorServiceImpl(), grpc_server
    )
    grpc_server.add_insecure_port(f"{args.host}:{args.grpc_port}")
    grpc_server.start()

    # Start HTTP server (blocking)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[primary_node] HTTP  listening on http://{args.host}:{args.port}")
    print(f"[primary_node] gRPC  listening on {args.host}:{args.grpc_port}")
    print("  HTTP endpoints:")
    print("    GET  /health")
    print("    GET  /nodes")
    print("    POST /register")
    print("    POST /compute")
    print("  gRPC services:")
    print("    CoordinatorService.RegisterNode")
    print("    CoordinatorService.ListNodes")
    print("    CoordinatorService.Compute")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
        grpc_server.stop(grace=5)
    finally:
        httpd.server_close()
        grpc_server.wait_for_termination(timeout=5)
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()
