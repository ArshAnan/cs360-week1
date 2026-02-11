## Week 02 – Part 1 Notes

This directory contains the **week 2** version of the distributed primes system, where we are migrating from HTTP+JSON to **gRPC + Protocol Buffers**.

This file documents **Part 1 (schema-first)** work, **Part 2 (WorkerService)**, **Part 3 (CoordinatorService)**, and the key pieces for **Part 4 (CLI gRPC path)** and **Part 5 (tests)**.

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

### Part 2 – Implement WorkerService

In `secondary_node.py`, the existing HTTP-based worker logic has been complemented with a **gRPC `WorkerService`** so that the coordinator can call workers over gRPC instead of HTTP/JSON.

#### What was implemented

- **`WorkerService` class** (subclasses the generated `primes_pb2_grpc.WorkerService` servicer):
  - **`ComputeRange` RPC** – reuses the same partitioning + thread/process execution model as the original `compute_partitioned()` function.
    - Unpacks fields from a `ComputeRangeRequest` protobuf message.
    - Converts protobuf enums (`MODE_COUNT`/`MODE_LIST`, `EXEC_SINGLE`/`EXEC_THREADS`/`EXEC_PROCESSES`) to the string values the existing helpers expect, then converts them back to enums when building the response.
    - Returns a typed `ComputeRangeResponse` (not a JSON dict), including optional `per_chunk` summaries and `primes` list.
  - **`Health` RPC** – returns a `HealthResponse` with `ok=True` and `status="healthy"`.

- **gRPC registration on startup** (`register_with_coordinator_grpc`):
  - If `--coordinator-grpc` is provided, the worker creates a `CoordinatorServiceStub` and calls `RegisterNode` once at startup with its `node_id`, advertised host/port, and CPU count.
  - Failures are logged but do not prevent the worker from starting (the coordinator may come up later).

- **Dual-protocol server in `main()`**:
  - The worker now starts **both** an HTTP server (legacy, on `--port`) and a gRPC server (on `--grpc-port`, default 50051).
  - Both are shut down gracefully on `KeyboardInterrupt`.

#### New CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-port` | `50051` | Port for the gRPC `WorkerService` listener. |
| `--coordinator-grpc` | _(none)_ | Coordinator gRPC address (e.g. `127.0.0.1:50050`). When set, the worker registers itself via `RegisterNode` on startup. |

#### Design note on enum handling

Protobuf enums arrive as integers (e.g. `MODE_COUNT = 0`), not strings. `ComputeRange` converts them to the string values (`"count"`, `"single"`, etc.) that the existing partition/execution helpers expect, runs the computation, and then converts back to enum values for the `ComputeRangeResponse`. This keeps the core prime-computation code unchanged from week01.

---

### Part 3 – Implement CoordinatorService

In `primary_node.py`, the existing HTTP-based coordinator has been complemented with a **gRPC `CoordinatorService`** so that the CLI (and workers) can interact with the coordinator over gRPC instead of HTTP/JSON. Both protocols run side-by-side and share the same in-memory node registry.

#### What was implemented

- **`CoordinatorServiceImpl` class** (subclasses the generated `primes_pb2_grpc.CoordinatorServiceServicer`):
  - **`RegisterNode` RPC** – accepts a `RegisterNodeRequest`, upserts the node into the shared `REGISTRY` (the same one the HTTP `/register` endpoint uses), and returns a `RegisterNodeResponse` containing the full `Node` record.
  - **`ListNodes` RPC** – returns all active (non-stale) nodes sorted by `node_id`, exactly mirroring the HTTP `GET /nodes` behavior.
  - **`Compute` RPC** – the main distributed computation entrypoint:
    1. Validates inputs (`high > low`); returns `INVALID_ARGUMENT` with a `BadRequest` field violation on failure.
    2. Converts protobuf enums (`MODE_COUNT`/`MODE_LIST`, `EXEC_SINGLE`/`EXEC_THREADS`/`EXEC_PROCESSES`) to the string values the existing helpers expect.
    3. Discovers active workers from `REGISTRY`; returns `FAILED_PRECONDITION` if none are registered.
    4. Splits `[low, high)` into per-node slices using the existing `split_into_slices()` helper.
    5. Fans out `ComputeRange` gRPC calls to worker nodes in parallel via `ThreadPoolExecutor`.
    6. Enforces a **120-second deadline** on each downstream `ComputeRange` RPC.
    7. Retries failed worker RPCs up to **3 times** with exponential backoff (1s, 2s, 4s).
    8. If **all** workers fail, aborts with `UNAVAILABLE` and structured `DebugInfo` details. If only **some** fail, continues with the successful results.
    9. Aggregates results exactly like the HTTP `distributed_compute()` path: sums `total_primes`, tracks `max_prime`, merges prime lists respecting `max_return_primes`, and computes timing metrics.
    10. Returns a typed `ComputeResponse` including optional `per_node` breakdown.

- **Structured gRPC error handling** (`_abort_with_status` helper):
  - Builds rich errors using the Google `google.rpc.Status` model.
  - Attaches `DebugInfo` (stack traces / detailed context) and `BadRequest` field violations as `google.protobuf.Any`-packed details.
  - Uses `grpc_status.rpc_status.to_status()` to convert to a gRPC-native status and calls `context.abort_with_status()`.

- **Downstream error conversion** (`_grpc_error_to_detail_string` helper):
  - Extracts status code, message, and any rich details from a downstream `grpc.RpcError`.
  - Used for logging and for building the aggregated error message when forwarding failures to the caller.

- **Dual-protocol server in `main()`**:
  - The coordinator now starts **both** an HTTP server (legacy, on `--port`) and a gRPC server (on `--grpc-port`, default 50050).
  - Both protocols share the same `REGISTRY`, so nodes registered via HTTP are visible to gRPC callers and vice versa.
  - Both are shut down gracefully on `KeyboardInterrupt` with a 5-second grace period.

#### New CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-port` | `50050` | Port for the gRPC `CoordinatorService` listener. |

#### Error mapping

| Condition | gRPC Status Code | Rich Detail |
|-----------|-------------------|-------------|
| `high <= low` | `INVALID_ARGUMENT` | `BadRequest` field violation on `high` |
| No active workers registered | `FAILED_PRECONDITION` | _(message only)_ |
| All downstream worker RPCs fail | `UNAVAILABLE` | `DebugInfo` with per-worker error strings |

#### Design note on shared state

The gRPC `CoordinatorServiceImpl` and the HTTP `Handler` both operate on the same global `REGISTRY` instance. This means a worker that registers via the HTTP `POST /register` endpoint is immediately available to a `Compute` RPC arriving over gRPC, and vice versa. The registry is protected by a threading lock and is safe for concurrent access from both server threads.

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
