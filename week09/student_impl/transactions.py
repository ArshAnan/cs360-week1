from __future__ import annotations

import json
from typing import Any, Protocol


class CoordinatorTransport(Protocol):
    def apply_to_shard(self, logical_shard_id: int, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    def read_from_shard(self, logical_shard_id: int, query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...


class RouterView(Protocol):
    def logical_shard_for_payload(self, application_name: str, operation_name: str, payload: dict[str, Any]) -> int:
        ...

    def owner_addr_for_logical_shard(self, logical_shard_id: int) -> str:
        ...


def _clone_state(state: dict[str, Any]) -> dict[str, Any]:
    # Shard state must remain JSON-serializable for the provided durable storage.
    return json.loads(json.dumps(state))


def _inventory_tables(state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    inventory = state.setdefault("inventory", {})
    reservations = state.setdefault("reservations", {})
    # Tombstone of reservation_ids that have been successfully released.
    # Keeping them durable lets retried release calls after a timeout
    # return committed=True instead of raising "unknown reservation".
    released_reservations = state.setdefault("released_reservations", {})
    if not all(isinstance(table, dict) for table in (inventory, reservations, released_reservations)):
        raise ValueError("inventory shard state is corrupted")
    return inventory, reservations, released_reservations


def _available_quantity(item_record: dict[str, Any]) -> int:
    total_quantity = int(item_record.get("total_quantity", 0))
    reserved_quantity = int(item_record.get("reserved_quantity", 0))
    return total_quantity - reserved_quantity


def _with_served_by(result: dict[str, Any]) -> dict[str, Any]:
    routing = result.get("routing")
    if routing is not None:
        result["served_by"] = [routing]
    return result


def execute_gateway_request(
    application_name: str,
    operation_name: str,
    payload: dict[str, Any],
    router: RouterView,
    transport: CoordinatorTransport,
) -> dict[str, Any]:
    """
    Execute one application request at the gateway layer.

    Students should implement the transaction logic for their chosen
    application here. The returned dictionary must be JSON-serializable.
    """
    if application_name != "inventory":
        raise NotImplementedError(f"application {application_name!r} is not implemented by this submission")

    logical_shard_id = router.logical_shard_for_payload(application_name, operation_name, payload)

    if operation_name == "create_item":
        return transport.apply_to_shard(logical_shard_id, operation_name, payload)

    if operation_name in {"reserve_item", "release_reservation"}:
        result = transport.apply_to_shard(logical_shard_id, operation_name, payload)
        return _with_served_by(result)

    if operation_name == "get_inventory":
        result = transport.read_from_shard(logical_shard_id, operation_name, payload)
        return _with_served_by(result)

    raise NotImplementedError(f"unsupported inventory operation {operation_name!r}")


def apply_local_mutation(state: dict[str, Any], operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Apply a single-shard mutation to local shard state and return a
    JSON-serializable result.
    """
    working_state = _clone_state(state)
    inventory, reservations, released_reservations = _inventory_tables(working_state)

    if operation_name == "create_item":
        item_id = str(payload["item_id"])
        quantity = int(payload["quantity"])
        if quantity < 0:
            raise ValueError("quantity must be non-negative")

        existing = inventory.get(item_id)
        if existing is None:
            inventory[item_id] = {
                "item_id": item_id,
                "total_quantity": quantity,
                "reserved_quantity": 0,
            }
        else:
            if int(existing.get("total_quantity", 0)) != quantity:
                raise ValueError(f"inventory item {item_id!r} already exists with different quantity")

        state.clear()
        state.update(working_state)
        return {
            "item_id": item_id,
            "quantity": int(inventory[item_id]["total_quantity"]),
        }

    if operation_name == "reserve_item":
        item_id = str(payload["item_id"])
        reservation_id = str(payload["reservation_id"])
        quantity = int(payload["quantity"])
        if quantity <= 0:
            raise ValueError("reservation quantity must be positive")

        item_record = inventory.get(item_id)
        if item_record is None:
            raise ValueError(f"unknown inventory item {item_id!r}")

        existing_reservation = reservations.get(reservation_id)
        if existing_reservation is not None:
            same_request = (
                str(existing_reservation.get("item_id")) == item_id
                and int(existing_reservation.get("quantity", 0)) == quantity
            )
            if not same_request:
                raise ValueError(f"reservation {reservation_id!r} already exists")
            return {
                "committed": True,
                "remaining_quantity": _available_quantity(item_record),
            }

        if reservation_id in released_reservations:
            raise ValueError(
                f"reservation {reservation_id!r} has already been released and cannot be reused"
            )

        if _available_quantity(item_record) < quantity:
            raise ValueError(f"not enough available inventory for {item_id!r}")

        item_record["reserved_quantity"] = int(item_record.get("reserved_quantity", 0)) + quantity
        reservations[reservation_id] = {
            "reservation_id": reservation_id,
            "item_id": item_id,
            "quantity": quantity,
        }

        state.clear()
        state.update(working_state)
        return {
            "committed": True,
            "remaining_quantity": _available_quantity(inventory[item_id]),
        }

    if operation_name == "release_reservation":
        item_id = str(payload["item_id"])
        reservation_id = str(payload["reservation_id"])

        existing_reservation = reservations.get(reservation_id)
        if existing_reservation is None:
            # Retry-after-success is safe: if this reservation_id was
            # already released, short-circuit and return committed=True.
            tombstone = released_reservations.get(reservation_id)
            if tombstone is not None:
                if str(tombstone.get("item_id")) != item_id:
                    raise ValueError(
                        f"reservation {reservation_id!r} does not belong to item {item_id!r}"
                    )
                item_record = inventory.get(item_id)
                if item_record is None:
                    raise ValueError(f"inventory item {item_id!r} is missing")
                return {
                    "committed": True,
                    "remaining_quantity": _available_quantity(item_record),
                }
            raise ValueError(f"unknown reservation {reservation_id!r}")
        if str(existing_reservation.get("item_id")) != item_id:
            raise ValueError(f"reservation {reservation_id!r} does not belong to item {item_id!r}")

        item_record = inventory.get(item_id)
        if item_record is None:
            raise ValueError(f"inventory item {item_id!r} is missing")

        quantity = int(existing_reservation.get("quantity", 0))
        new_reserved_quantity = int(item_record.get("reserved_quantity", 0)) - quantity
        if new_reserved_quantity < 0:
            raise ValueError(f"inventory item {item_id!r} has invalid reserved quantity")

        item_record["reserved_quantity"] = new_reserved_quantity
        released_reservations[reservation_id] = {
            "reservation_id": reservation_id,
            "item_id": item_id,
            "quantity": quantity,
        }
        del reservations[reservation_id]

        state.clear()
        state.update(working_state)
        return {
            "committed": True,
            "remaining_quantity": _available_quantity(inventory[item_id]),
        }

    raise NotImplementedError(f"unsupported local mutation {operation_name!r}")


def run_local_query(state: dict[str, Any], query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Execute a single-shard read against local shard state and return a
    JSON-serializable result.
    """
    inventory, _, _ = _inventory_tables(state)

    if query_name == "get_inventory":
        item_id = str(payload["item_id"])
        item_record = inventory.get(item_id)
        if item_record is None:
            raise ValueError(f"unknown inventory item {item_id!r}")

        total_quantity = int(item_record.get("total_quantity", 0))
        reserved_quantity = int(item_record.get("reserved_quantity", 0))
        return {
            "item_id": item_id,
            "total_quantity": total_quantity,
            "reserved_quantity": reserved_quantity,
            "available_quantity": total_quantity - reserved_quantity,
        }

    raise NotImplementedError(f"unsupported local query {query_name!r}")
