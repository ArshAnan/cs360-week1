"""
This is an interface that allows you to interact with the system more simply.
Here you can start a primary node, stop a primary node, and send compute tasks to the primary node
To use it simply run uv run week01/system_interface.py
"""
import inquirer
import subprocess
import requests as r
import time

PRIMARY_DEFAULT = "http://127.0.0.1:9200"

def post_json(url: str, payload: dict, timeout_s: int = 3600) -> dict:
    response = r.post(url, json=payload, timeout=timeout_s)
    response.raise_for_status()
    return response.json()

def main():
    print("WELCOME TO THE DISTRIBUTED SYSTEM")

    primary_proc = None
    primary_url = PRIMARY_DEFAULT

    while True:
        print()
        questions = [
            inquirer.List(
                "system_command",
                message="Select a System Command: ",
                choices=[
                    "Start Primary Node",
                    "Send Request To System",
                    "Quit Program",
                ],
            )
        ]

        answers = inquirer.prompt(questions)

        if not answers:
            continue

        match answers["system_command"]:
            case "Start Primary Node":
                # non-blocking start
                if primary_proc and primary_proc.poll() is None:
                    print("Primary Node is already running.")
                    continue

                primary_proc = subprocess.Popen(
                    ["uv", "run", "primary_node.py"],
                    cwd="week01",
                    stdout=None,
                    stderr=None,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
          
                print(f"Primary Node starting on {primary_url}")

            case "Send Request To System":
                print("\nNOTE:")
                print("- This menu only supports DISTRIBUTED compute.")
                print("- Secondary nodes perform the processing; the primary coordinates work.")
                print("- Make sure to have at least 1 secondary node running or this request will fail.")
                print("- You may use secondary_node_interface.py to cordinate secondary nodes.")

                # Ask only for low, high, mode
                req_questions = [
                    inquirer.Text(
                        "low",
                        message="Enter low (inclusive)",
                        validate=lambda _, x: x.strip().lstrip("-").isdigit(),
                    ),
                    inquirer.Text(
                        "high",
                        message="Enter high (exclusive)",
                        validate=lambda _, x: x.strip().lstrip("-").isdigit(),
                    ),
                    inquirer.List(
                        "mode",
                        message="Select mode",
                        choices=["count", "list"],
                    ),
                ]
                req = inquirer.prompt(req_questions)
                if not req:
                    continue

                low = int(req["low"])
                high = int(req["high"])
                mode = req["mode"]

                if high <= low:
                    print("Error: high must be > low")
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

                url = primary_url.rstrip("/") + "/compute"

                try:
                    t0 = time.perf_counter()
                    resp = post_json(url, payload, timeout_s=3600)
                    t1 = time.perf_counter()
                except Exception as e:
                    print(f"Network error sending request to {url}: {e}")
                    print()
                    continue

                if not resp.get("ok"):
                    print(f"Distributed error response: {resp}")
                    print()
                    continue

                if mode == "count":
                    print(f'Prime count : {int(resp.get("total_primes", 0))}')
                else:
                    primes = list(resp.get("primes", []))
                    total = int(resp.get("total_primes", len(primes)))

                    shown = primes[:50]  # cap output
                    print(f"Total primes: {total}")
                    print(f"First {len(shown)} primes (from returned sample):")
                    print(" ".join(map(str, shown)))
                    if resp.get("primes_truncated") or total > len(primes):
                        print(f"... (returned primes are capped at {resp.get('max_return_primes', 5000)})")

                print(f"(Distributed elapsed: {t1 - t0:.6f}s)\n")
                print()

            case "Quit Program":
                if primary_proc and primary_proc.poll() is None:
                    primary_proc.terminate()
                break

if __name__ == "__main__":
    main()
