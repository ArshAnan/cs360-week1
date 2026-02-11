#!/usr/bin/env python3
"""
test_primes_grpc.py

Task 5 tests for the week02 gRPC-based prime system.

We focus on:
- Count correctness for known ranges.
- List correctness + truncation behavior.
- Error mapping for "no active workers".
- Error mapping for bad input (high <= low).
- One integration-style test with 1 primary + 2 workers.
"""

from __future__ import annotations

import threading
import time
import unittest
from typing import Tuple

import grpc

import primary_node
import secondary_node
import primes_pb2
import primes_pb2_grpc


def _start_primary_grpc(host: str = "127.0.0.1", port: int = 60050) -> Tuple[grpc.Server, str]:
    """
    Start a primary gRPC server only (no HTTP) on the given host/port.
    Returns (server, address_string).
    """
    # Fresh registry per test to avoid state leakage.
    primary_node.REGISTRY = primary_node.Registry(ttl_s=3600)

    server = grpc.server(primary_node.ThreadPoolExecutor(max_workers=10))
    primes_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        primary_node.CoordinatorServiceImpl(), server
    )
    address = f"{host}:{port}"
    server.add_insecure_port(address)
    server.start()
    return server, address


def _start_worker_grpc(host: str = "127.0.0.1", port: int = 61000) -> Tuple[grpc.Server, str]:
    """
    Start a worker gRPC server on the given host/port.
    Returns (server, address_string).
    """
    server = grpc.server(secondary_node.ThreadPoolExecutor(max_workers=10))
    primes_pb2_grpc.add_WorkerServiceServicer_to_server(
        secondary_node.WorkerService(), server
    )
    address = f"{host}:{port}"
    server.add_insecure_port(address)
    server.start()
    return server, address


def _register_worker_with_primary(
    coordinator_addr: str,
    node_id: str,
    worker_addr: str,
) -> None:
    """
    Register a worker (identified by worker_addr host:port) with the
    coordinator at coordinator_addr using RegisterNode.
    """
    host, port_str = worker_addr.split(":")
    port = int(port_str)

    channel = grpc.insecure_channel(coordinator_addr)
    stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
    req = primes_pb2.RegisterNodeRequest(
        node_id=node_id,
        host=host,
        port=port,
        cpu_count=1,
        ts=time.time(),
    )
    stub.RegisterNode(req, timeout=5.0)
    channel.close()


class TestCoordinatorGrpc(unittest.TestCase):
    def test_count_correctness_small_range(self) -> None:
        """
        Count correctness for a small known range using 1 primary + 1 worker.
        Primes in [0, 20): 2,3,5,7,11,13,17,19 -> 8 primes.
        """
        primary_srv, coord_addr = _start_primary_grpc(port=60051)
        worker_srv, worker_addr = _start_worker_grpc(port=61001)
        try:
            _register_worker_with_primary(coord_addr, "worker-1", worker_addr)

            channel = grpc.insecure_channel(coord_addr)
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            req = primes_pb2.ComputeRequest(
                low=0,
                high=20,
                mode=primes_pb2.MODE_COUNT,
                chunk=10,
                secondary_exec=primes_pb2.EXEC_SINGLE,
                secondary_workers=0,
                max_return_primes=0,
                include_per_node=False,
            )
            resp = stub.Compute(req, timeout=10.0)
            channel.close()

            self.assertTrue(resp.ok)
            self.assertEqual(resp.total_primes, 8)
        finally:
            worker_srv.stop(grace=0)
            primary_srv.stop(grace=0)

    def test_list_correctness_and_truncation(self) -> None:
        """
        List correctness + truncation for a small range.
        In [0, 100), there are 25 primes; we cap at max_return_primes=5.
        """
        primary_srv, coord_addr = _start_primary_grpc(port=60052)
        worker_srv, worker_addr = _start_worker_grpc(port=61002)
        try:
            _register_worker_with_primary(coord_addr, "worker-1", worker_addr)

            channel = grpc.insecure_channel(coord_addr)
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            req = primes_pb2.ComputeRequest(
                low=0,
                high=100,
                mode=primes_pb2.MODE_LIST,
                chunk=50,
                secondary_exec=primes_pb2.EXEC_SINGLE,
                secondary_workers=0,
                max_return_primes=5,
                include_per_node=False,
            )
            resp = stub.Compute(req, timeout=10.0)
            channel.close()

            self.assertTrue(resp.ok)
            # Known first few primes under 100
            expected_prefix = [2, 3, 5, 7, 11]
            self.assertEqual(list(resp.primes), expected_prefix)
            self.assertEqual(resp.max_return_primes, 5)
            self.assertTrue(resp.primes_truncated)
            # Total primes under 100 is 25
            self.assertEqual(resp.total_primes, 25)
        finally:
            worker_srv.stop(grace=0)
            primary_srv.stop(grace=0)

    def test_no_active_workers_error(self) -> None:
        """
        When no workers are registered, Compute should fail with
        FAILED_PRECONDITION.
        """
        primary_srv, coord_addr = _start_primary_grpc(port=60053)
        try:
            channel = grpc.insecure_channel(coord_addr)
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            req = primes_pb2.ComputeRequest(
                low=0,
                high=10,
                mode=primes_pb2.MODE_COUNT,
                chunk=5,
                secondary_exec=primes_pb2.EXEC_SINGLE,
                secondary_workers=0,
                max_return_primes=0,
                include_per_node=False,
            )
            with self.assertRaises(grpc.RpcError) as ctx:
                stub.Compute(req, timeout=5.0)
            err = ctx.exception
            self.assertEqual(err.code(), grpc.StatusCode.FAILED_PRECONDITION)
            self.assertIn("no active secondary nodes registered", err.details() or "")
            channel.close()
        finally:
            primary_srv.stop(grace=0)

    def test_bad_input_high_le_low_error(self) -> None:
        """
        When high <= low, Compute should fail with INVALID_ARGUMENT.
        """
        primary_srv, coord_addr = _start_primary_grpc(port=60054)
        worker_srv, worker_addr = _start_worker_grpc(port=61004)
        try:
            _register_worker_with_primary(coord_addr, "worker-1", worker_addr)

            channel = grpc.insecure_channel(coord_addr)
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            req = primes_pb2.ComputeRequest(
                low=10,
                high=10,
                mode=primes_pb2.MODE_COUNT,
                chunk=5,
                secondary_exec=primes_pb2.EXEC_SINGLE,
                secondary_workers=0,
                max_return_primes=0,
                include_per_node=False,
            )
            with self.assertRaises(grpc.RpcError) as ctx:
                stub.Compute(req, timeout=5.0)
            err = ctx.exception
            self.assertEqual(err.code(), grpc.StatusCode.INVALID_ARGUMENT)
            self.assertIn("high must be > low", err.details() or "")
            channel.close()
        finally:
            worker_srv.stop(grace=0)
            primary_srv.stop(grace=0)

    def test_integration_primary_with_two_workers(self) -> None:
        """
        Simple integration-style test: 1 primary + 2 workers registered,
        distributed count over [0, 100).
        """
        primary_srv, coord_addr = _start_primary_grpc(port=60055)
        worker1_srv, worker1_addr = _start_worker_grpc(port=61005)
        worker2_srv, worker2_addr = _start_worker_grpc(port=61006)
        try:
            _register_worker_with_primary(coord_addr, "worker-1", worker1_addr)
            _register_worker_with_primary(coord_addr, "worker-2", worker2_addr)

            channel = grpc.insecure_channel(coord_addr)
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            req = primes_pb2.ComputeRequest(
                low=0,
                high=100,
                mode=primes_pb2.MODE_COUNT,
                chunk=50,
                secondary_exec=primes_pb2.EXEC_SINGLE,
                secondary_workers=0,
                max_return_primes=0,
                include_per_node=True,
            )
            resp = stub.Compute(req, timeout=10.0)
            channel.close()

            self.assertTrue(resp.ok)
            # There are 25 primes under 100.
            self.assertEqual(resp.total_primes, 25)
            # We registered 2 workers; coordinator should use both.
            self.assertEqual(resp.nodes_used, 2)
            # Per-node breakdown should also reflect 2 entries.
            self.assertEqual(len(resp.per_node), 2)
        finally:
            worker2_srv.stop(grace=0)
            worker1_srv.stop(grace=0)
            primary_srv.stop(grace=0)


if __name__ == "__main__":
    unittest.main()

