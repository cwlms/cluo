from cluo.config import RunMode, set_run_mode
from cluo.core import Batch, Record
from cluo.sinks import LoggerSink


def test_logger_sink_write(capsys):
    set_run_mode(RunMode.WRITE)
    s = LoggerSink()
    batch = Batch([Record(data={"a": 1}), Record(data={"b": 2})])
    s(batch)
    captured = capsys.readouterr()
    # there could be more logged out than just this
    assert "LoggerSink Batch(" in captured.out


def test_logger_sink_preview(capsys):
    set_run_mode(RunMode.PREVIEW)
    s = LoggerSink()
    batch = Batch([Record(data={"a": 1}), Record(data={"b": 2})])
    s(batch)
    captured = capsys.readouterr()
    # there could be more logged out than just this
    assert "LoggerSink Batch(" in captured.out
