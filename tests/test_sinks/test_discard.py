from cluo.config import RunMode, set_run_mode
from cluo.core import Batch, Record
from cluo.sinks import DiscardSink


def test_discard_sink_write():
    set_run_mode(RunMode.WRITE)
    s = DiscardSink()
    batch = Batch([Record(data={"a": 1}), Record(data={"b": 2})])
    assert batch == s(batch)


def test_discard_sink_preview():
    set_run_mode(RunMode.PREVIEW)
    s = DiscardSink()
    batch = Batch([Record(data={"a": 1}), Record(data={"b": 2})])
    assert batch == s(batch)
