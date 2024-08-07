# Core concepts

cluo is Techstars' a stream processing library, with Airflow-like control flow construction syntax. It makes it easy to build composable stream processing pipelines, complete with branching, conditional logic, useful data transformation stages, etc.

To get started, you only need to know a few core concepts:

A **batch** is a collection of records that get passed through a pipeline as well as accompanying metadata (see `cluo.core.Batch` object). A **record** is a single dictionary (string keys) that contain data for one entity. The number of records in a batch is configurable and records can hcluoe different paths through a pipeline. This means that the number of records in a batch can change between stages in a pipeline.

**Stages** (`cluo.stages.<StageName>`)are the basic unit of work in a pipeline. Stages are children of the `cluo.core.Stage` base class. Each stage only contains information about its own work and pointers to downstream stages. This decouples stages from one another, making them easy to compose and customize.

**Sources** (`cluo.sources.<SourceName>`) and **sinks** (`cluo.sources.<SinkName>`) are special types of stages that manage data reads and writes, respectively. They are distinct from the general case stage in order to 1) hold special read-write logic, 2) allow for different treatment by the pipeline object, and 3) serve as a logically distinct unit of work for the developer.

The **pipeline** (`cluo.core.Pipeline`) should be considered a manager of sources, sinks, stages, and the batch/record control flow process. Any time pipeline behcluoior depends on more than one stage or state of the data at more than one point in time, the pipeline is in charge.
