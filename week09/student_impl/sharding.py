from __future__ import annotations

from typing import Any

import hashlib

TOTAL_LOGICAL_SHARDS = 8

# dont use hash(key) % current_number_of_nodes because when we add/remove nodes, the hash values will change and cause a lot of keys to be remapped to different nodes. This is called "hash churn" and can lead to performance issues. Instead, we can use a consistent hashing algorithm that maps keys to a fixed number of logical shards, and then map those logical shards to physical nodes. This way, when we add/remove nodes, only a small number of keys will be remapped to different nodes, reducing hash churn and improving performance.
def build_partition_key(application_name: str, operation_name: str, payload: dict[str, Any]) -> str:
    """
    Return the partition key used to route a request.

    Students should implement a partition-key choice that matches the
    selected application's workload and explain the tradeoffs in
    student_impl/README.md.
    """
    item_id = payload.get("item_id")
    if item_id is not None:
        return str(item_id)
    
    for field in ("reservation_id", "account_id", "section_id", "from_account_id", "to_account_id","student_id"):
        value = payload.get(field)
        if value is not None:
            return str(value)

    return f"{application_name}:{operation_name}"

def choose_logical_shard(partition_key: str, total_logical_shards: int = TOTAL_LOGICAL_SHARDS) -> int:
    """
    Map a partition key to a logical shard id in the range [0, total_logical_shards).

    The tests will check that your sharding function spreads a representative
    set of keys relatively evenly across the available logical shards.
    """

    raise NotImplementedError("Implement choose_logical_shard() in student_impl/sharding.py")
