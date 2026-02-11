## Week 02 – Part 1 Notes

This directory contains the **week 2** version of the distributed primes system, where we are migrating from HTTP+JSON to **gRPC + Protocol Buffers**.

This file documents **Part 1 (schema-first)** work and the key pieces for **Part 4 (CLI gRPC path)** and **Part 5 (tests)**.

---

### What was implemented in Part 1

- **New protobuf schema** at `proto/primes.proto`.
  - Defines the enums:
    - `Mode` (`MODE_COUNT`, `MODE_LIST`) – replaces the old `"count"` / `"list"` strings.
    - `ExecMode` (`EXEC_SINGLE`, `EXEC_THREADS`, `EXEC_PROCESSES`) – replaces `"single"`, `"threads"`, `"processes"`.
  - Defines all messages and services needed for the rest of the assignment:
    - **Worker side** (`WorkerService`):
      - `ComputeRangeRequest` / `ComputeRangeResponse`
      - `ChunkSummary`
      - `HealthRequest` / `HealthResponse`
      - These are shaped to match the existing `secondary_node.py` HTTP `/compute` behavior and `compute_partitioned(...)` result.
    - **Coordinator side** (`CoordinatorService`):
      - `RegisterNodeRequest` / `RegisterNodeResponse`
      - `ListNodesRequest` / `ListNodesResponse`
      - `PerNodeResult`
      - `ComputeRequest` / `ComputeResponse`
      - These are shaped to match the current `primary_node.py` `/register`, `/nodes`, and `/compute` JSON I/O and what the CLI expects.

The goal was to keep the **business logic unchanged** and only formalize the network API in a typed `.proto` file so future parts can plug gRPC in without changing prime logic.

---

### How to generate the gRPC Python code

From the `week02` directory, run:

```bash
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=. \
  --grpc_python_out=. \
  ./proto/primes.proto
```

This creates:

- `primes_pb2.py` – message & enum definitions.
- `primes_pb2_grpc.py` – service base classes and stubs.

These generated files will be used in later parts of the project (servers + CLI). Do **not** edit them by hand; re-run the command above if the `.proto` changes.

---

### Part 4 – gRPC CLI (Distributed Mode)

In week02, the CLI’s distributed execution path has been migrated from HTTP/JSON to **gRPC**.

- **Local modes unchanged**:
  - `--exec single`
  - `--exec threads`
  - `--exec processes`
- **Distributed mode updated**:
  - `--exec distributed` now calls the coordinator’s `Compute` RPC on `CoordinatorService` (defined in `proto/primes.proto`) instead of POSTing to `/compute`.

The CLI still accepts the same flags as week01 and keeps the same output format as much as possible:

- `--low`, `--high`, `--mode {count,list}`
- `--chunk`, `--workers`
- `--exec {single,threads,processes,distributed}`
- Distributed-only: `--primary`, `--secondary-exec`, `--secondary-workers`, `--include-per-node`, `--max-return-primes`, `--time`

#### Setup / Dependencies for gRPC

Install the gRPC / protobuf dependencies (once per environment):

```bash
python -m pip install grpcio grpcio-tools protobuf
```

Regenerate protobuf stubs if `proto/primes.proto` changes:

```bash
cd week02
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=. \
  --grpc_python_out=. \
  ./proto/primes.proto
```

This produces `primes_pb2.py` and `primes_pb2_grpc.py` in `week02/`, which are imported by `primary_node.py`, `secondary_node.py`, and `primes_cli.py`.

#### Running the gRPC system (demo flow)

1. **Start the coordinator (primary gRPC + HTTP server)**:

   ```bash
   cd week02
   python primary_node.py --host 127.0.0.1 --port 9200 --grpc-port 50050
   ```

2. **Start one or more workers (secondary nodes)** in separate terminals, registering to the coordinator via gRPC:

   ```bash
   cd week02
   python secondary_node.py \
     --host 127.0.0.1 \
     --port 9100 \
     --grpc-port 50051 \
     --coordinator-grpc 127.0.0.1:50050
   ```

3. **Run the CLI in distributed mode (via gRPC)**:

   ```bash
   cd week02
   python primes_cli.py \
     --low 0 --high 100000 \
     --mode count \
     --exec distributed \
     --primary 127.0.0.1:50050 \
     --secondary-exec processes \
     --time \
     --include-per-node
   ```

Notes:

- `--primary` can be passed as `127.0.0.1:50050` or `http://127.0.0.1:50050`; the CLI normalizes it to a gRPC target `host:port`.
- In **count** mode, the CLI prints just the total prime count.
- In **list** mode, it prints total primes and the first `--max-print` primes, mirroring the week01 behavior.
- With `--time` and `--include-per-node`, the CLI shows per-node timing and slice information aggregated from the coordinator’s `Compute` response.

---

### Part 5 – Tests for gRPC path

Task 5 adds automated tests in `test_primes_grpc.py` to validate the new gRPC-based system:

- **Count correctness** for known ranges.
- **List correctness + truncation** behavior.
- **No active workers** error mapping.
- **Bad input (high <= low)** error mapping.
- One **integration test** with 1 primary + 2 workers.

#### How the tests work

- The tests start **in-process** gRPC servers:
  - A primary using `CoordinatorServiceImpl` from `primary_node.py`.
  - Workers using `WorkerService` from `secondary_node.py`.
- Workers are registered to the primary via the `RegisterNode` RPC.
- Tests then call `CoordinatorService.Compute` and assert on:
  - Total prime counts.
  - Returned prime lists and truncation flags.
  - gRPC status codes and error messages for failure cases.

#### Running the tests

From the `week02` directory:

```bash
python -m unittest test_primes_grpc.py
```

Make sure you have installed the gRPC dependencies and generated `primes_pb2.py` / `primes_pb2_grpc.py` before running the tests.
