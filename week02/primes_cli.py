#!/usr/bin/env python3
"""
primes_cli.py

Notes
-----
- Examples of how to run from terminal: 
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec single --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec threads --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec processes --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec distributed --time --mode count --secondary-exec processes --primary http://127.0.0.1:9200
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import grpc 
import primes_pb2
import primes_pb2_grpc
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any
from primes_in_range import get_primes


#convert exec mode string to primes_pb2.ExecMode enum to be used in the gRPC request
def convert_exec_mode(mode: str) -> primes_pb2.ExecMode:
    if mode == "single":
        return primes_pb2.EXEC_SINGLE
    elif mode == "threads":
        return primes_pb2.EXEC_THREADS
    else:
        return primes_pb2.EXEC_PROCESSES

    


def iter_ranges(low: int, high: int, chunk: int) -> List[Tuple[int, int]]:
    """Split [low, high) into contiguous chunks."""
    if chunk <= 0:
        raise ValueError("--chunk must be > 0")
    out: List[Tuple[int, int]] = []
    x = low
    while x < high:
        y = min(x + chunk, high)
        out.append((x, y))
        x = y
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Tuple[int, int, object]:
    a, b, return_list = args
    res = get_primes(a, b, return_list=return_list)
    return (a, b, res)





def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Prime counting/listing over [low, high) using local threads/processes OR distributed secondary nodes."
    )
    ap.add_argument("--low", type=int, required=True, help="Range start (inclusive).")
    ap.add_argument("--high", type=int, required=True, help="Range end (exclusive). Must be > low.")
    ap.add_argument("--mode", choices=["list", "count"], default="count")
    ap.add_argument("--chunk", type=int, default=500_000)
    ap.add_argument("--exec", choices=["single", "threads", "processes", "distributed"], default="single")
    ap.add_argument("--workers", type=int, default=(os.cpu_count() or 4))
    ap.add_argument("--max-print", type=int, default=50)
    ap.add_argument("--time", action="store_true")

    # Distributed options
    ap.add_argument("--primary", default=None, help="Primary URL, e.g. http://127.0.0.1:9200 or 127.0.0.1:50050")
    ap.add_argument("--protocol", choices=["grpc", "http"], default="grpc",
                    help="Protocol to use for distributed compute (default: grpc).")
    ap.add_argument("--secondary-exec", choices=["single", "threads", "processes"], default="processes")
    ap.add_argument("--secondary-workers", type=int, default=None)
    ap.add_argument("--include-per-node", action="store_true")
    ap.add_argument("--max-return-primes", type=int, default=5000)

    args = ap.parse_args(argv)

    if args.high <= args.low:
        print("Error: --high must be > --low", file=sys.stderr)
        return 2

    return_list = (args.mode == "list")

    if args.exec == "distributed":
        if not args.primary:
            print("Error: --primary is required when --exec distributed", file=sys.stderr)
            return 2

        protocol = args.protocol
        print(f"[cli] Using protocol: {protocol.upper()}", file=sys.stderr)

        t0 = time.perf_counter()
        payload = {
            "low": args.low,
            "high": args.high,
            "mode": "list" if return_list else "count",
            "chunk": args.chunk,
            "secondary_exec": args.secondary_exec,
            "secondary_workers": args.secondary_workers,
            "max_return_primes": args.max_return_primes,
            "include_per_node": args.include_per_node,
        }

        if protocol == "grpc":
            # ---- gRPC path ----
            target = args.primary.replace("http://", "").rstrip("/")  # strip http:// to get host:port for gRPC channel
            try:
                channel = grpc.insecure_channel(target)  # create gRPC channel
                stub = primes_pb2_grpc.CoordinatorServiceStub(channel)  # create gRPC stub for Coordinator service

                request = primes_pb2.ComputeRequest(  # request mimicking the ComputeRequest message defined in primes.proto
                    low=int(payload["low"]),
                    high=int(payload["high"]),
                    mode=primes_pb2.MODE_COUNT if payload["mode"] == "count" else primes_pb2.MODE_LIST,
                    chunk=int(payload.get("chunk")),
                    secondary_exec=convert_exec_mode(payload.get("secondary_exec", "processes")),
                    secondary_workers=payload.get("secondary_workers") or 0,
                    max_return_primes=int(payload.get("max_return_primes")),
                    include_per_node=bool(payload.get("include_per_node")),
                )

                response = stub.Compute(request, timeout=3600)
                resp = {  # convert gRPC response to a dictionary for uniform handling below
                    "ok": response.ok,
                    "error": response.error,
                    "total_primes": response.total_primes,
                    "max_prime": response.max_prime,
                    "primes": list(response.primes),
                    "primes_truncated": response.primes_truncated,
                    "nodes_used": response.nodes_used,
                    "per_node": response.per_node,
                    "elapsed_seconds": response.elapsed_seconds,
                }

                channel.close()
            except grpc.RpcError as e:  # connection issues or server errors
                print(f"gRPC error: {e.details()}", file=sys.stderr)
                return 1

        else:
            # ---- HTTP path ----
            primary_url = args.primary if args.primary.startswith("http") else f"http://{args.primary}"
            primary_url = primary_url.rstrip("/")
            url = f"{primary_url}/compute"

            http_payload = {k: v for k, v in payload.items() if v is not None}
            data = json.dumps(http_payload).encode("utf-8")
            req = urllib.request.Request(url, data=data, method="POST")
            req.add_header("Content-Type", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=3600) as http_resp:
                    resp = json.loads(http_resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                error_body = e.read().decode("utf-8", errors="replace")
                print(f"HTTP error {e.code} {e.reason}: {error_body}", file=sys.stderr)
                return 1
            except Exception as e:
                print(f"HTTP error: {e}", file=sys.stderr)
                return 1

        t1 = time.perf_counter()

        if not resp.get("ok"):
            print(f"Distributed error: {resp}", file=sys.stderr)
            return 1

        if args.mode == "count":
            print(int(resp.get("total_primes", 0)))
        else:
            primes = list(resp.get("primes", []))
            total = int(resp.get("total_primes", len(primes)))
            shown = primes[: args.max_print]
            print(f"Total primes: {total}")
            print(f"First {len(shown)} primes (from returned sample):")
            print(" ".join(map(str, shown)))
            if resp.get("primes_truncated") or total > len(primes):
                print(f"... (returned primes are capped at {resp.get('max_return_primes', args.max_return_primes)})")

        if args.time:
            print(
                f"Elapsed seconds: {t1 - t0:.6f}  "
                f"(exec=distributed, protocol={protocol.upper()}, nodes_used={resp.get('nodes_used')}, "
                f"secondary_exec={resp.get('secondary_exec')}, chunk={args.chunk})",
                file=sys.stderr,
            )
            if args.include_per_node and "per_node" in resp:
                print("Per-node summary:", file=sys.stderr)
                if protocol == "grpc":
                    for node in resp["per_node"]:
                        print(
                            f"  {node.node_id:>12}: primes={node.total_primes}  "
                            f"node_elapsed={node.node_elapsed_s:.3f}s  round_trip={node.round_trip_s:.3f}s",
                            file=sys.stderr,
                        )
                else:
                    for node in resp["per_node"]:
                        print(
                            f"  {node.get('node_id','?'):>12}: primes={node.get('total_primes',0)}  "
                            f"node_elapsed={node.get('node_elapsed_s',0):.3f}s  round_trip={node.get('round_trip_s',0):.3f}s",
                            file=sys.stderr,
                        )
        return 0

    # Local paths
    ranges = iter_ranges(args.low, args.high, args.chunk)
    t0 = time.perf_counter()
    results: List[Tuple[int, int, object]] = []

    if args.exec == "single":
        for a, b in ranges:
            results.append(_work_chunk((a, b, return_list)))

    elif args.exec == "threads":
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, return_list)) for a, b in ranges]
            for f in as_completed(futs):
                results.append(f.result())

    else:  # processes
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, return_list)) for a, b in ranges]
            for f in as_completed(futs):
                results.append(f.result())

    t1 = time.perf_counter()
    results.sort(key=lambda x: x[0])

    if args.mode == "count":
        total = 0
        for _, _, res in results:
            total += int(res)  # type: ignore[arg-type]
        print(total)
    else:
        all_primes: List[int] = []
        for _, _, res in results:
            all_primes.extend(list(res))  # type: ignore[arg-type]
        total = len(all_primes)
        shown = all_primes[: args.max_print]
        print(f"Total primes: {total}")
        print(f"First {len(shown)} primes:")
        print(" ".join(map(str, shown)))
        if total > len(shown):
            print(f"... ({total - len(shown)} more not shown)")

    if args.time:
        print(
            f"Elapsed seconds: {t1 - t0:.6f}  "
            f"(exec={args.exec}, workers={args.workers if args.exec!='single' else 1}, chunks={len(ranges)}, chunk_size={args.chunk})",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))