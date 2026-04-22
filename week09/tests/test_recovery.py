from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
RUNTIME_DIR = ROOT / ".runtime"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"


def _wait_for_port(addr: str, timeout: float = 15.0) -> None:
    host, raw_port = addr.rsplit(":", 1)
    port = int(raw_port)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"Timed out waiting for {addr}")


def _run_script(script_name: str) -> None:
    proc = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / script_name)],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"{script_name} failed (rc={proc.returncode})\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )


def _restart_cluster(gateway_addr: str) -> None:
    _run_script("stop_cluster.py")
    _run_script("run_cluster.py")
    _wait_for_port(gateway_addr, timeout=15.0)


def _fresh_gateway_stub(gateway_addr: str):
    from conftest import import_stubs

    _, _, gateway_pb2, gateway_grpc = import_stubs()
    channel = grpc.insecure_channel(gateway_addr)
    grpc.channel_ready_future(channel).result(timeout=10.0)
    return gateway_pb2, gateway_grpc.Week09GatewayStub(channel)


def test_item_and_reservation_survive_cluster_restart(selected_application, cluster, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("recovery tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(
        gateway_pb2.CreateInventoryItemRequest(item_id="item-recov-1", quantity=5),
        timeout=5.0,
    )
    stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-recov-1", reservation_id="r-recov-1", quantity=2),
        timeout=5.0,
    )

    _restart_cluster(cluster["gateway"]["addr"])
    gateway_pb2_after, stub_after = _fresh_gateway_stub(cluster["gateway"]["addr"])

    inventory = stub_after.GetInventory(
        gateway_pb2_after.GetInventoryRequest(item_id="item-recov-1"),
        timeout=5.0,
    )
    assert inventory.total_quantity == 5
    assert inventory.reserved_quantity == 2
    assert inventory.available_quantity == 3


def test_released_reservation_tombstone_survives_restart(selected_application, cluster, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("recovery tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(
        gateway_pb2.CreateInventoryItemRequest(item_id="item-recov-2", quantity=3),
        timeout=5.0,
    )
    stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-recov-2", reservation_id="r-recov-2", quantity=1),
        timeout=5.0,
    )
    release = stub.ReleaseReservation(
        gateway_pb2.ReleaseReservationRequest(item_id="item-recov-2", reservation_id="r-recov-2"),
        timeout=5.0,
    )
    assert release.committed is True

    _restart_cluster(cluster["gateway"]["addr"])
    gateway_pb2_after, stub_after = _fresh_gateway_stub(cluster["gateway"]["addr"])

    replayed = stub_after.ReleaseReservation(
        gateway_pb2_after.ReleaseReservationRequest(item_id="item-recov-2", reservation_id="r-recov-2"),
        timeout=5.0,
    )
    assert replayed.committed is True
    assert replayed.remaining_quantity == 3

    inventory = stub_after.GetInventory(
        gateway_pb2_after.GetInventoryRequest(item_id="item-recov-2"),
        timeout=5.0,
    )
    assert inventory.reserved_quantity == 0
    assert inventory.available_quantity == 3


def test_retry_release_is_idempotent_without_restart(selected_application, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("recovery tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(
        gateway_pb2.CreateInventoryItemRequest(item_id="item-recov-3", quantity=2),
        timeout=5.0,
    )
    stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-recov-3", reservation_id="r-recov-3", quantity=1),
        timeout=5.0,
    )
    first = stub.ReleaseReservation(
        gateway_pb2.ReleaseReservationRequest(item_id="item-recov-3", reservation_id="r-recov-3"),
        timeout=5.0,
    )
    assert first.committed is True

    retried = stub.ReleaseReservation(
        gateway_pb2.ReleaseReservationRequest(item_id="item-recov-3", reservation_id="r-recov-3"),
        timeout=5.0,
    )
    assert retried.committed is True
    assert retried.remaining_quantity == 2


def test_tombstoned_reservation_id_cannot_be_reused(selected_application, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("recovery tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(
        gateway_pb2.CreateInventoryItemRequest(item_id="item-recov-4", quantity=2),
        timeout=5.0,
    )
    stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-recov-4", reservation_id="r-recov-4", quantity=1),
        timeout=5.0,
    )
    stub.ReleaseReservation(
        gateway_pb2.ReleaseReservationRequest(item_id="item-recov-4", reservation_id="r-recov-4"),
        timeout=5.0,
    )

    with pytest.raises(Exception):
        stub.ReserveItem(
            gateway_pb2.ReserveItemRequest(item_id="item-recov-4", reservation_id="r-recov-4", quantity=1),
            timeout=5.0,
        )

    inventory = stub.GetInventory(
        gateway_pb2.GetInventoryRequest(item_id="item-recov-4"),
        timeout=5.0,
    )
    assert inventory.reserved_quantity == 0
    assert inventory.available_quantity == 2
