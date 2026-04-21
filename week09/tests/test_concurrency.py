from __future__ import annotations

import concurrent.futures

import pytest


def test_last_unit_race_has_exactly_one_winner(selected_application, gateway_stub):
    """
    A capacity-1 item should admit exactly one concurrent reservation winner.
    This exercises the per-logical-shard lock in the shard server and validates
    the declared SERIALIZABLE_LIKE isolation for the most important
    check-then-act anomaly in the inventory workload.
    """
    if selected_application != "inventory":
        pytest.skip("concurrency tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    item_id = "item-race-1"
    thread_count = 16

    stub.CreateInventoryItem(
        gateway_pb2.CreateInventoryItemRequest(item_id=item_id, quantity=1),
        timeout=5.0,
    )

    def try_reserve(index: int) -> tuple[int, bool, str | None]:
        try:
            response = stub.ReserveItem(
                gateway_pb2.ReserveItemRequest(
                    item_id=item_id,
                    reservation_id=f"r-race-{index}",
                    quantity=1,
                ),
                timeout=5.0,
            )
            return index, bool(response.committed), None
        except Exception as exc:
            return index, False, f"{type(exc).__name__}: {exc}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as pool:
        results = list(pool.map(try_reserve, range(thread_count)))

    winners = [(idx, err) for idx, committed, err in results if committed]
    losers = [(idx, err) for idx, committed, err in results if not committed]

    assert len(winners) == 1, (
        f"expected exactly one committed reservation, got {len(winners)}.\n"
        f"winners: {winners}\nlosers: {losers}"
    )
    assert len(losers) == thread_count - 1
    for _, error_message in losers:
        assert error_message is not None, "losing reservations must raise, not silently succeed"

    inventory = stub.GetInventory(
        gateway_pb2.GetInventoryRequest(item_id=item_id),
        timeout=5.0,
    )
    assert inventory.total_quantity == 1
    assert inventory.reserved_quantity == 1
    assert inventory.available_quantity == 0
