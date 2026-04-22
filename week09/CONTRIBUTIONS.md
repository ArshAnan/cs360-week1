# Contributions

This document records the primary ownership area of each team member on the
week09 disk-backed sharding + transactions project. The git history is the
source of truth for exact commits; this file summarises the high-level
workstreams.

## Stephanie Mata

Primary area: **Shard routing and partition key selection.**

- Chose `item_id` as the partition key for the inventory application and
  declared `HASH_DISTRIBUTED` in `student_impl/project_choice.py`.
- Implemented `build_partition_key(...)` and `choose_logical_shard(...)` in
  `student_impl/sharding.py`, using SHA-256 over the partition key mapped
  into a fixed logical shard space of 8 shards.
- Switched `SELECTED_APPLICATION` from course_registration to inventory in
  `project_choice.py` once the team agreed on the application.
- Representative commits: `42f73f4`, `845d676`, `4fb7b12`, `05c61f3`,
  `aa2098a`, `f763ddb`, `220d78a`.

## Arsh Anand

Primary area: **Transaction coordinator and commit protocol.**

- Implemented `execute_gateway_request(...)` in `student_impl/transactions.py`,
  routing each inventory operation to a single owning shard.
- Implemented `apply_local_mutation(...)` for `create_item`, `reserve_item`,
  and `release_reservation` with staged-copy-then-commit atomicity.
- Implemented `run_local_query(...)` for `get_inventory`.
- Established single-shard-only transaction scope for the chosen workload
  and the supporting invariants (non-negative available quantity,
  idempotent reservation on matching `reservation_id`).
- Representative commit: `ba4e1a8`.

## Arnav Deepaware

Primary area: **Durable storage layer and crash-write semantics.**

- Hardened `save_logical_shard_state(...)` in `student_impl/storage.py`:
  writes to a `.tmp` sibling, `fsync` the file, `os.replace` to the
  destination, and then `fsync` the parent directory so the rename itself
  is durable.
- Added input validation that rejects non-dict shard states.
- Documented the crash-point behaviour in `student_impl/README.md` and the
  rationale for gateway-stateless operation under crash.
- Representative commit: `94d95ac`.

## Kalelo Dukuray

Primary area: **Failure handling, recovery, and integration testing.**

- Added a durable `released_reservations` tombstone in
  `student_impl/transactions.py` so that retried `release_reservation`
  calls after a successful release are safe and return `committed=True`.
- Added rejection of reservation-id reuse after release.
- Added `tests/test_recovery.py` exercising full cluster restart via
  `scripts/stop_cluster.py` and `scripts/run_cluster.py`, verifying that
  inventory, reservations, and release tombstones all survive a restart.
- Added `tests/test_concurrency.py` exercising the per-shard lock through
  a last-unit race on a capacity-1 item.
- Added `tests/test_storage_crash_safety.py` with direct unit coverage of
  the provided storage primitives' crash-resilience properties.
- Updated `student_impl/README.md` with retry/idempotency and testing
  sections, and authored this `CONTRIBUTIONS.md`.

## Shared responsibilities

All team members participated in:

- choosing the inventory application and the single-shard transaction model
- reviewing pull requests and testing integration points
- preparing the in-class presentation
