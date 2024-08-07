# Branch and Advanced Flow Walk through

Throughout this process we will walk through creating a pipeline that has conditional branching and uses some of the more advanced flow control syntax.

## Requirements
This walk through assumes that you have gone through building your first pipeline. If not watch the video on creating your first pipeline and/or look through the files in `examples/intro_to_cluo/my_first_pipeline`.


## Building the pipeline 

### Copy and Modify my_first_pipeline Example
Copy the code from `my_first_pipeline/my_first_pipeline.py` then we will make the following changes:
* We will need to make sure to change the names to more accurately reflect our new pipeline.
* Update the kafka source to use a different topic
* Add two additional logger sinks
* add a branching stage.
  * define b_is_12 function
  * create branch with correct mapping
* Modify the flow
  * Use a dictionary to assign downstream stages to channels
  * Use lists to assign multiple downstreams stages at once

### Test the Pipeline
Similar to how we tested the first pipeline we do the following:
* bring up the dockers containers defined in `tests`
* create a kafka topic
* start a kafka consumer
* start a kafka producer
* start the pipeline
* pass in some example records and observe how they are handled

Below is a list of all commands used in testing:
```shell
docker-compose up -d
kafka-topics --create --topic test.branchingpipeline --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --list --bootstrap-server localhost:9092
kafka-console-consumer --topic test.branchingpipeline --from-beginning --bootstrap-server localhost:9092
kafka-console-producer --topic test.branchingpipeline  --bootstrap-server localhost:9092
poetry run python my_first_pipeline.py
docker-compose down
```

Then here is a list of the test records we use to test with:
```shell
{"a": "hello", "b": 12}
{"a": "hello", "b": 13}
{"a": "hello"}
{"a": "hello", "b": 12, "c": 12}
{"a": "hello", "b": 13, "c": 12}
{"a": "hello", "c": 12}
{"b": 12, "c": 12}
{"b": 13, "c": 12}
{"c": 12}
```









