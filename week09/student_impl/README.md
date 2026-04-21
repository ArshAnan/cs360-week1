# Student Implementation README

## Selected application

This submission implements the provided `inventory` application.

The supported application operations are:

- `create_item`
- `reserve_item`
- `release_reservation`
- `get_inventory`

## Partition key and sharding strategy

The partition key is `item_id`.

All inventory state for a given item, including the item's total quantity and
all reservations against that item, is stored on the same logical shard. The
gateway computes the logical shard from the `item_id` and routes the request to
that shard.

The shard mapping uses SHA-256 over the partition key and then maps the digest
into a fixed logical shard space. This keeps the routing stable at the logical
shard level and avoids the naive `hash(key) % current_number_of_nodes` pattern.

## Why this partition key fits the workload

For the inventory workload, the critical invariant is that reservations for one
item must not exceed that item's available quantity. Sharding by `item_id`
keeps all conflicting updates for the same item on one shard, so the system can
enforce that invariant without needing cross-shard coordination.

This is a good fit because the application's important operations are naturally
centered on a single item:

- create one item
- reserve quantity from one item
- release one reservation for one item
- read one item's inventory summary

## Distribution goal

The declared sharding tradeoff is `hash_distributed`.

Using SHA-256 on `item_id` aims to spread independent items relatively evenly
across the fixed set of logical shards, which matches the provided workload
better than a range-locality strategy.

## Transaction scope

Transactions are single-shard only for the selected inventory application.

Because all state for one item is colocated by `item_id`, each logical
operation can execute entirely on the owning shard:

- `create_item` updates one item record
- `reserve_item` checks availability and records the reservation on the same shard
- `release_reservation` removes the reservation and restores availability on the same shard

## Isolation declaration

The declared isolation tradeoff is `serializable_like`.

The implementation approximates this by serializing reads and writes per
logical shard in the shard server. Only one operation at a time can load,
inspect, modify, and persist a shard's durable state, which prevents
check-then-act races for reservations on the same item.

## Atomicity

Atomicity is achieved in two layers:

1. Local mutation logic stages changes in a copied shard-state object and only
   publishes the new state when all validation succeeds.
2. The shard server persists the full updated shard state to disk only after
   `apply_local_mutation(...)` returns successfully.

If validation fails midway through a reservation or release, the mutation raises
an error and the shard server does not save partial state.

## Isolation

Isolation is achieved with a per-logical-shard lock in the shard server.

That lock serializes:

- writes against the same shard
- reads that would otherwise race with concurrent writes

For the selected application, this prevents the important anomalies the tests
look for, such as two concurrent reservations both believing the same inventory
is still available.

## Anomalies prevented

This design is intended to prevent:

- partial reservation updates
- partial release updates
- over-allocation of one item's quantity
- lost updates between competing reservation attempts on the same item
- reads observing half-applied inventory state

## Use of the provided storage layer

The implementation uses the provided disk-backed storage functions in
`student_impl/storage.py`.

Each shard node:

- loads durable JSON state for one logical shard before executing a request
- applies one local transaction against that in-memory state
- saves the updated shard state with an atomic write pattern using a temp file,
   `fsync`, `os.replace`, and parent-directory `fsync`

Because committed state is written to the shard's storage file, committed data
survives process restarts.

## Recovery behavior

Recovery is straightforward for this single-shard inventory design:

- when a shard process restarts, it reloads the last committed JSON state for
  each logical shard it owns
- reservation and item records are reconstructed directly from that durable state
- operations that failed before the save step leave no committed partial effects

Crash-point behavior for one mutation request:

- crash before `save_logical_shard_state(...)` starts:
   no durable change is visible after restart
- crash after temp-file write but before `os.replace(...)`:
   previous committed shard file remains authoritative after restart
- crash after `os.replace(...)` and directory `fsync`:
   the new committed shard state is durable after restart

Coordinator/gateway crash handling:

- the gateway is stateless for inventory operations and does not hold unflushed
   transaction state
- if a client retries after a timeout, request outcomes remain safe because shard
   state is loaded from disk on each request and reservation IDs enforce
   idempotency for duplicate reserve calls

There is no separate cross-shard transaction log because the selected workload
does not require cross-shard transactions.

## Retry and idempotency

Client retries after a timeout are a first-class part of the failure model, so
each mutation is designed to be safe to replay:

- `create_item` is idempotent when the same `(item_id, quantity)` pair is
  re-sent; a conflicting quantity raises instead of silently overwriting.
- `reserve_item` is idempotent on `reservation_id`. A retry that presents the
  same `(item_id, quantity)` returns `committed=True` without mutating state.
  A retry that presents a different payload for an existing `reservation_id`
  is rejected to prevent accidental id reuse.
- `release_reservation` writes a durable tombstone into a
  `released_reservations` table on the shard before deleting the live
  reservation record. A later retry for the same `reservation_id` finds the
  tombstone and returns `committed=True` with the current available quantity,
  instead of raising "unknown reservation". The tombstone is stored in the
  same per-shard JSON file that is written through the hardened storage
  layer, so it survives process restarts.
- A `reservation_id` that has been released cannot be re-used for a new
  reservation; `reserve_item` rejects it explicitly.

Together these rules make every supported operation safe to retry after a
gateway timeout, a shard restart, or any other interruption that leaves the
client uncertain about the outcome.

## How this is tested

The submission adds three test modules on top of the provided suite:

- `tests/test_recovery.py`
  Starts the cluster, writes inventory and reservation state, then runs
  `scripts/stop_cluster.py` followed by `scripts/run_cluster.py` to force a
  full process restart. After the restart the test reconnects to the same
  gateway address and asserts that item quantities, reservations, and
  release tombstones all survived. It also covers in-process retry of a
  release and rejection of reuse of a released `reservation_id`.
- `tests/test_concurrency.py`
  Creates a capacity-1 item and launches 16 threads that race to reserve
  the last unit. The test asserts exactly one reservation commits and the
  final reserved and available quantities match, exercising the per-shard
  lock in `shard_server.StudentShardStoreAdapter`.
- `tests/test_storage_crash_safety.py`
  Direct unit tests of the provided storage primitives: missing files load
  as empty, a stale `.json.tmp` next to a real `.json` does not corrupt the
  load path, a successful save leaves no orphan temp file, a second save
  atomically replaces the previous file, and malformed top-level JSON or a
  non-dict state value is rejected.

## Known limitations

- Only the `inventory` application is implemented in this submission.
- The design does not implement distributed commit across multiple shards.
- The release tombstone currently grows without bound; a production system
  would add a background trimming policy keyed by age or by reservation
  epoch.
