import datetime

import pytest

from cluo.core.offset_manager import OffsetType
from cluo.offset_managers import InMemoryOffsetManager


def test_first_run_no_offset():
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT)
    offset_manager.set_key("test_first_run_no_offset")
    offset_manager.set(1)

    assert offset_manager.get() == 1


def test_datetime_offset():
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.DATETIME)
    offset_manager.set_key("test_datetime_offset")
    offset_manager.set(datetime.datetime(2021, 1, 1, 0, 0, 0))

    assert offset_manager.get() == datetime.datetime(2021, 1, 1, 0, 0, 0)


def test_str_offset():
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.STR)
    offset_manager.set_key("test_datetime_offset")
    offset_manager.set("A")

    assert offset_manager.get() == "A"


def test_offset_update():
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.INT.STR)
    offset_manager.set_key("test_string_offset")
    offset_manager.set("A")

    assert offset_manager.get() == "A"

    offset_manager.set("B")

    assert offset_manager.get() == "B"


def test_type_error():
    offset_manager = InMemoryOffsetManager(offset_type=OffsetType.STR)
    offset_manager.set_key("test_value_error_offset")

    with pytest.raises(ValueError):
        offset_manager.set(1)
