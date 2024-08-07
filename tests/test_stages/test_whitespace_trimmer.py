import pytest

from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.stages import WhitespaceTrimmerStage
from tests.conftest import parametrize_processes


@parametrize_processes(WhitespaceTrimmerStage)
def test_whitespace_is_trimmed(processes):
    stage = WhitespaceTrimmerStage(fields=["a"], processes=processes)
    input_ = Batch(records=[Record(data={"a": "  hello  "})])
    expected_output = Batch(records=[Record(data={"a": "hello"})])
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)


@parametrize_processes(WhitespaceTrimmerStage)
def test_error_on_bad_type(processes):
    stage = WhitespaceTrimmerStage(
        fields=["a"],
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[Record(data={"a": 13})])
    with pytest.raises(AttributeError):
        stage(input_)


@parametrize_processes(WhitespaceTrimmerStage)
def test_no_field(processes):
    stage = WhitespaceTrimmerStage(
        fields=["a"],
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[Record(data={"b": "hello"})])
    expected_output = Batch(records=[Record(data={"b": "hello"})])
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)


@parametrize_processes(WhitespaceTrimmerStage)
def test_none_field(processes):
    stage = WhitespaceTrimmerStage(
        fields=["a"],
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[Record(data={"a": None, "b": "hello"})])
    expected_output = Batch(records=[Record(data={"a": None, "b": "hello"})])
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)
