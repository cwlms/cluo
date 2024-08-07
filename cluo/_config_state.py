# type: ignore
import logging
import sys

from cluo.run_mode import RunMode

config_state = sys.modules[__name__]
config_state.LOG_LEVEL = logging.INFO
config_state.RUN_MODE = RunMode.PREVIEW
