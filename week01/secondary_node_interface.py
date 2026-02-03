"""
This is an interface for managing secondary nodes. 
To use it simply run uv run week01/secondary_node_interface.py
"""
import inquirer
import subprocess

PRIMARY_DEFAULT = "http://127.0.0.1:9200"
BASE_PORT_DEFAULT = 9100

def main():
    print("----- Secondary Node Launcher -----")

    primary_url = PRIMARY_DEFAULT
    procs: dict[str, subprocess.Popen] = {}

    while True:
        questions = [
            inquirer.List(
                "cmd",
                message="Secondary Node Options:",
                choices=[
                    "Start 1 Secondary Node",
                    "Start N Secondary Nodes",
                    "List Running Secondary Nodes (started here)",
                    "Stop a Secondary Node (started here)",
                    "Quit",
                ],
            )
        ]
        ans = inquirer.prompt(questions)
        if not ans:
            continue

        match ans["cmd"]:
            case "Start 1 Secondary Node":
                qs = [
                    inquirer.Text("node_id", message="Node ID", validate=lambda _, x: len(x.strip()) > 0),
                    inquirer.Text(
                        "port",
                        message=f"Port (default {BASE_PORT_DEFAULT})",
                        default=str(BASE_PORT_DEFAULT),
                        validate=lambda _, x: x.strip().isdigit(),
                    ),
                ]
                a = inquirer.prompt(qs)
                if not a:
                    continue

                node_id = a["node_id"].strip()
                port = int(a["port"])

                if node_id in procs and procs[node_id].poll() is None:
                    print(f"{node_id} is already running.\n")
                    continue

                p = subprocess.Popen(
                    ["uv", "run", "secondary_node.py", "--primary", primary_url, "--node-id", node_id, "--port", str(port)],
                    cwd="week01",
                    stdout=None,
                    stderr=None,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
                procs[node_id] = p
                print(f"Started secondary node '{node_id}' on port {port} (registering to {primary_url}).\n")

            case "Start N Secondary Nodes":
                qs = [
                    inquirer.Text(
                        "count",
                        message="How many secondary nodes?",
                        validate=lambda _, x: x.strip().isdigit() and int(x) > 0,
                    ),
                    inquirer.Text(
                        "base_port",
                        message=f"Base port (default {BASE_PORT_DEFAULT})",
                        default=str(BASE_PORT_DEFAULT),
                        validate=lambda _, x: x.strip().isdigit(),
                    ),
                    inquirer.Text(
                        "prefix",
                        message="Node ID prefix (e.g. node)",
                        default="node",
                        validate=lambda _, x: len(x.strip()) > 0,
                    ),
                ]
                a = inquirer.prompt(qs)
                if not a:
                    continue

                count = int(a["count"])
                base_port = int(a["base_port"])
                prefix = a["prefix"].strip()

                for i in range(count):
                    node_id = f"{prefix}{i+1}"
                    port = base_port + i

                    if node_id in procs and procs[node_id].poll() is None:
                        print(f"Skipping {node_id}: already running.")
                        continue

                    p = subprocess.Popen(
                        ["uv", "run", "secondary_node.py", "--primary", primary_url, "--node-id", node_id, "--port", str(port)],
                        cwd="week01",
                        stdout=None,
                        stderr=None,
                        stdin=subprocess.DEVNULL,
                        start_new_session=True,
                    )
                    procs[node_id] = p

                print(f"Started {count} secondary nodes.\n")

            case "List Running Secondary Nodes (started here)":
                running = [nid for nid, p in procs.items() if p.poll() is None]
                if not running:
                    print("None running (from this launcher).\n")
                else:
                    print("Running secondary nodes (from this launcher):")
                    for nid in running:
                        print(f" - {nid}")
                    print()

            case "Stop a Secondary Node (started here)":
                running = [nid for nid, p in procs.items() if p.poll() is None]
                if not running:
                    print("No running nodes to stop.\n")
                    continue

                q = [
                    inquirer.List("node_id", message="Select a node to stop", choices=running)
                ]
                a = inquirer.prompt(q)
                if not a:
                    continue

                nid = a["node_id"]
                p = procs[nid]
                p.terminate()
                print(f"Stopped {nid}.\n")

            case "Quit":
                for nid, p in list(procs.items()):
                    if p.poll() is None:
                        p.terminate()
                break

if __name__ == "__main__":
    main()
