from __future__ import annotations

import json

import pytest

from student_impl import storage


def test_load_returns_empty_for_missing_file(tmp_path):
    missing = tmp_path / "does_not_exist.json"
    assert storage.load_logical_shard_state(missing) == {}


def test_load_returns_empty_for_blank_file(tmp_path):
    blank = tmp_path / "blank.json"
    blank.write_text("   \n\n")
    assert storage.load_logical_shard_state(blank) == {}


def test_stale_tmp_file_does_not_affect_load(tmp_path):
    """
    If a prior crash left a .json.tmp next to the real .json, the load path
    must still return the authoritative state in the .json file. The .tmp
    file is abandoned and has no effect on readers.
    """
    storage_path = tmp_path / "logical_shard_0.json"
    valid_state = {"inventory": {"item-a": {"total_quantity": 4, "reserved_quantity": 1}}}
    storage.save_logical_shard_state(storage_path, valid_state)

    stale_tmp = storage_path.with_suffix(storage_path.suffix + ".tmp")
    stale_tmp.write_bytes(b"{this-is-not-valid-json")

    loaded = storage.load_logical_shard_state(storage_path)
    assert loaded == valid_state
    assert stale_tmp.exists(), "this test deliberately leaves the stale .tmp in place"


def test_save_is_atomic_replace(tmp_path):
    """
    A second save_logical_shard_state must atomically replace the previous
    file contents, not merge with or partially overwrite them.
    """
    storage_path = tmp_path / "logical_shard_1.json"
    first = {"inventory": {"item-a": {"total_quantity": 1, "reserved_quantity": 0}}}
    second = {
        "inventory": {"item-b": {"total_quantity": 2, "reserved_quantity": 1}},
        "reservations": {"r-1": {"item_id": "item-b", "quantity": 1}},
    }

    storage.save_logical_shard_state(storage_path, first)
    assert storage.load_logical_shard_state(storage_path) == first

    storage.save_logical_shard_state(storage_path, second)
    reloaded = storage.load_logical_shard_state(storage_path)
    assert reloaded == second
    assert "item-a" not in reloaded.get("inventory", {})


def test_save_leaves_no_tmp_file_after_success(tmp_path):
    storage_path = tmp_path / "logical_shard_2.json"
    storage.save_logical_shard_state(storage_path, {"inventory": {}})
    stale_tmp = storage_path.with_suffix(storage_path.suffix + ".tmp")
    assert storage_path.exists()
    assert not stale_tmp.exists(), "successful save should os.replace the temp file, leaving none behind"


def test_save_rejects_non_dict(tmp_path):
    storage_path = tmp_path / "logical_shard_3.json"
    with pytest.raises(ValueError):
        storage.save_logical_shard_state(storage_path, ["not", "a", "dict"])  # type: ignore[arg-type]


def test_load_rejects_non_object_top_level(tmp_path):
    storage_path = tmp_path / "logical_shard_4.json"
    storage_path.write_text(json.dumps(["not", "an", "object"]))
    with pytest.raises(ValueError):
        storage.load_logical_shard_state(storage_path)
