import datetime
import os
from pathlib import Path

import pytest

from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import FileOffsetManager

folder = "tests/offsets"


@pytest.fixture
def setup_teardown():
    # create offset folder
    os.makedirs(folder, exist_ok=True)

    yield

    # cleanup files
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        os.remove(file_path)

    # cleanup folder
    os.rmdir(folder)


def test_first_run_no_offset(setup_teardown):
    offset_manager = FileOffsetManager(
        offset_type=OffsetType.INT, filepath=f"{folder}/test_first_run_no_offset.txt"
    )
    offset_manager.set(1)

    assert offset_manager.get() == 1


def test_datetime_offset(setup_teardown):
    offset_manager = FileOffsetManager(
        offset_type=OffsetType.DATETIME, filepath=f"{folder}/test_datetime_offset.txt"
    )
    offset_manager.set_key("test_datetime_offset")
    offset_manager.set(datetime.datetime(2021, 1, 1, 0, 0, 0))

    assert offset_manager.get() == datetime.datetime(2021, 1, 1, 0, 0, 0)


def test_str_offset(setup_teardown):
    offset_manager = FileOffsetManager(
        offset_type=OffsetType.STR, filepath=f"{folder}/test_str_offset.txt"
    )
    offset_manager.set_key("test_str_offset")
    offset_manager.set("A")

    assert offset_manager.get() == "A"


def test_offset_update(setup_teardown):
    offset_manager = FileOffsetManager(
        offset_type=OffsetType.STR, filepath=f"{folder}/test_offset_update.txt"
    )
    offset_manager.set_key("test_offset_update")
    offset_manager.set("A")

    assert offset_manager.get() == "A"

    offset_manager.set("B")

    assert offset_manager.get() == "B"


def test_type_error(setup_teardown):
    offset_manager = FileOffsetManager(
        offset_type=OffsetType.STR, filepath=f"{folder}/test_value_error_offset.txt"
    )
    offset_manager.set_key("test_value_error_offset")

    with pytest.raises(ValueError):
        offset_manager.set(1)


def test_path_input(setup_teardown):
    path = Path(f"{folder}/test_path_input.txt")
    offset_manager = FileOffsetManager(offset_type=OffsetType.STR, filepath=path)
    offset_manager.set_key("test_path_input")

    offset_manager.set("1")
    assert offset_manager.get() == "1"
