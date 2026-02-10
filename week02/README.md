## Week 02 – Part 1 Notes

This directory contains the **week 2** version of the distributed primes system, where we are migrating from HTTP+JSON to **gRPC + Protocol Buffers**.

This file documents only **Part 1 (schema-first)** work.

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


