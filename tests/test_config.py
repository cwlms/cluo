import logging

from cluo.config import RunMode, config_state, set_logging_level, set_run_mode
from cluo.stages import IdentityStage


def test_set_logging_level():
    # test the default
    assert config_state.LOG_LEVEL == logging.INFO
    istage = IdentityStage()
    assert istage._log.level == logging.INFO

    # then after setting
    set_logging_level(logging.DEBUG)
    assert config_state.LOG_LEVEL == logging.DEBUG
    assert istage._log.level == logging.DEBUG


def test_set_run_mode():
    # test the default
    assert config_state.RUN_MODE == RunMode.PREVIEW
    # then after setting
    set_run_mode(RunMode.WRITE)
    assert config_state.RUN_MODE == RunMode.WRITE
