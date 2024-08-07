# Getting Started

The best way to get started is by looking at the pipeline `./examples/` in examples.

Let's build a small branching pipeline with a custom component.

First, some imports:

```python
import logging

from cluo import config
from cluo.core import Pipeline, Stage
from cluo.data_types import Record
from cluo.sinks import LoggerSink
from cluo.sources import DummySource
from cluo.stages import RenameFieldsStage
```

Now, before doing anything else, let's set the logging level to `logging.BATCH`. While verbose, this allows us to see each batch logged out as the pipeline runs, which can help with debugging. When you import cluo, this gets added on to Python's standard `logging` object.

```python
config.set_logging_level("BATCH")
```

We need some data to work with, so we'll initiate a source. The `DummySource` is nice for mocking out data during development without needing to connect to an external data source like PostgresQL or Kafka.

All stages (including sources and sinks) take a few optional keyword arguments, including `name` argument.

```python
dummy_source = DummySource(record={"a": 1, "b": 2, "key": "value"}, batch_size=10, name="my dummy source")
```

We'll instantiate two stages, including one custom stage. We'll string them all at the end.

Let's can rename the key `"key"` to `"new"`. All stages also take a `processes` argument (default: 1), which allows you to use multiprocessing for compute intensive stages. Most of the time this isn't necessary and may even be slower due to the overhead of spinning up multiple processes, but let's add it in for demonstration purposes:
```python
rename_stage = RenameFieldsStage(mapping={"key": "new"}, name="Key to new", processes=4)
```

We'll also create our own stage. For this, all we need to do is inherit the `Stage` class and implement a `.process_record` or `.process_batch` method that takes and returns a `Record` type or `Batch` object, respectively. (Notice the `.__init__` function body. You'll need to follow this pattern)

```python
class ExponentiationStage(Stage):
    def __init__(self, exponent: int, name: str | None = None,
        processes: int = 1,) -> None:
        Stage.__init__(self, name=name, processes=processes)
        self.exponent = exponent

    def process_record(self, record: Record) -> Record:
        record["a"] **= self.exponent
        record["b"] **= self.exponent

exp_stage = ExponentiationStage(exponent=2)
```

For the last stages, we'll create two sinks to log out the data, as well as demonstrate some fan-out branching.

```python
logger1 = LoggerSink(name="logger1")
logger2 = LoggerSink(name="logger2")
```

Finally, let's setup the pipeline flow.
```python
(dummy_source >> rename_stage >> exp_stage >> [logger1, logger2])
```

To get the pipeline to run, instantiate a `Pipeline`, set the pipelines data source(s), and run the pipeline:

```python
if __name__ == "__main__":
    pipeline = Pipeline("my first pipeline")
    pipeline.add_source(dummy_source)
    pipeline.run()
```