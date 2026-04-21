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
  `fsync`, and `os.replace`

Because committed state is written to the shard's storage file, committed data
survives process restarts.

## Recovery behavior

Recovery is straightforward for this single-shard inventory design:

- when a shard process restarts, it reloads the last committed JSON state for
  each logical shard it owns
- reservation and item records are reconstructed directly from that durable state
- operations that failed before the save step leave no committed partial effects

There is no separate cross-shard transaction log because the selected workload
does not require cross-shard transactions.

## Known limitations

- Only the `inventory` application is implemented in this submission.
- The design does not implement distributed commit across multiple shards.
- There is no special retry-tombstone mechanism for repeated release requests
  after a successful release.
