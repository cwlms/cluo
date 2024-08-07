import logging

from cluo import config
from cluo.core import Batch, Record
from cluo.core.stage import Stage
from cluo.stages import IdentityStage
from cluo.utils.log import LoggingMixin


def test_mode_is_globally_changeable_and_propogates_to_children_classes():
    config.set_logging_level(logging.INFO)
    logging_mixin = LoggingMixin()
    stage = Stage()
    assert logging_mixin.log.level == logging.INFO
    assert stage.log.level == logging.INFO

    config.set_logging_level(logging.DEBUG)
    assert logging_mixin.log.level == logging.DEBUG
    assert stage.log.level == logging.DEBUG

    config.set_logging_level(logging.WARNING)
    assert logging_mixin.log.level == logging.WARNING
    assert stage.log.level == logging.WARNING


def test_info_level(caplog):
    config.set_logging_level(logging.INFO)
    s = IdentityStage(name="s")
    batch = Batch(records=[Record(data={"a": 1}), Record(data={"b": 2})])
    s(batch)
    assert "Processing batch with IdentityStage stage: s" in caplog.text


def test_debug_level_shows_batch(caplog):
    config.set_logging_level("BATCH")
    s = IdentityStage(name="s")
    batch = Batch(
        records=[Record(data={"a": 1, "b": 2}), Record(data={"a": 3, "b": 4})]
    )
    s(batch)
    assert "Batch(\n\tRecord(data={'a': 1, 'b': 2}" in caplog.text
