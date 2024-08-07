import pytest

from cluo.core import Batch, ErrorHandlingMethod, Record
from cluo.core.exceptions import StageError
from cluo.stages import CleanCurrencyStage
from tests.conftest import parametrize_processes


@parametrize_processes(CleanCurrencyStage)
def test_call1(processes):
    stage = CleanCurrencyStage(fields=["a"], processes=processes)
    input_ = Batch(records=[Record(data={"a": "  usd  "})])
    expected_output = Batch(records=[Record(data={"a": "USD"})])
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)


@parametrize_processes(CleanCurrencyStage)
def test_call2(processes):
    stage = CleanCurrencyStage(fields=["a"], processes=processes)
    input_ = Batch(records=[Record(data={"a": "  FF  "})])
    expected_output = Batch(records=[Record(data={"a": "FF"})])
    actual_output = stage(input_)
    assert actual_output.has_same_record_data(expected_output)


@parametrize_processes(CleanCurrencyStage)
def test_error_on_bad_type(processes):
    stage = CleanCurrencyStage(
        fields=["a"],
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[Record(data={"a": 13})])
    with pytest.raises(AttributeError):
        stage(input_)


@parametrize_processes(CleanCurrencyStage)
def test_error_on_null_field(processes):
    stage = CleanCurrencyStage(
        fields=["a"],
        allow_nulls=False,
        processes=processes,
        error_handling_method=ErrorHandlingMethod.RAISE,
    )
    input_ = Batch(records=[Record(data={"a": None})])
    with pytest.raises(StageError):
        stage(input_)
