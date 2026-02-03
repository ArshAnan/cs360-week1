"""
This is an interface for interacting with a running primary node
To use it simply run uv run week01/primary_node_interface.py
"""
import time
from pprint import pprint
import requests as r

PRIMARY_URL = "http://127.0.0.1:9200"

def safe_get(url: str, timeout: int = 10) -> dict | None:
    try:
        resp = r.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Network error GET {url}: {e}\n")
        return None

def safe_post(url: str, payload: dict, timeout: int = 3600) -> dict | None:
    try:
        resp = r.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Network error POST {url}: {e}\n")
        return None

def prompt_int(prompt: str) -> int | None:
    s = input(prompt).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        print("Please enter a valid integer.\n")
        return None

def main():
    print("Terminal to Interact with Primary Node")
    print("Supported Commands:")
    print("  h - health")
    print("  n - list nodes")
    print("  r - register node (manual)")
    print("  c - distributed compute")
    print("  q - quit\n")

    while True:
        cmd = input("Type in a Command: ").strip().lower()
        if cmd == "q":
            break

        response = None

        match cmd:
            case "h":
                response = safe_get(f"{PRIMARY_URL}/health")

            case "n":
                response = safe_get(f"{PRIMARY_URL}/nodes")

            case "r":
                print("\nManual register (POST /register)")
                print("Tip: secondaries normally auto-register; this is for testing.\n")

                node_id = input("node_id (required): ").strip()
                host = input("host (required, e.g. 127.0.0.1): ").strip()
                port = prompt_int("port (required): ")
                if not node_id or not host or port is None:
                    print("Missing required fields (node_id, host, port).\n")
                    continue

                cpu_count = prompt_int("cpu_count (optional, press Enter to skip): ")
                payload = {
                    "node_id": node_id,
                    "host": host,
                    "port": port,
                    "cpu_count": cpu_count if cpu_count is not None else 1,
                    "ts": time.time(),
                }

                response = safe_post(f"{PRIMARY_URL}/register", payload, timeout=10)

            case "c":
                print("\nDistributed compute (POST /compute)")
                print("- Primary coordinates work across registered secondary nodes.")
                print("- This shell asks only for low/high/mode.\n")

                low = prompt_int("low (inclusive): ")
                high = prompt_int("high (exclusive): ")
                if low is None or high is None:
                    print("low and high are required.\n")
                    continue
                if high <= low:
                    print("Error: high must be > low\n")
                    continue

                mode = input("mode (count/list): ").strip().lower()
                if mode not in ("count", "list"):
                    print("Error: mode must be 'count' or 'list'\n")
                    continue

                payload = {
                    "low": low,
                    "high": high,
                    "mode": mode,
                    "chunk": 500_000,
                    "secondary_exec": "processes",
                    "secondary_workers": None,
                    "max_return_primes": 5000,
                    "include_per_node": False,
                }

                t0 = time.perf_counter()
                response = safe_post(f"{PRIMARY_URL}/compute", payload, timeout=3600)
                t1 = time.perf_counter()

                if response and response.get("ok"):
                    print(f"(Distributed elapsed: {t1 - t0:.6f}s)\n")

            case _:
                print("Unknown command. Use h / n / r / c / q.\n")
                continue

        if response is not None:
            pprint(response, indent=2)
            print()
        else:
            print("No response.\n")

if __name__ == "__main__":
    main()
