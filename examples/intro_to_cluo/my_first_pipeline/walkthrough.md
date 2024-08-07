# My First Pipeline Walkthrough

## Before you start checklist
* Make sure cluo is setup and the test all pass.
* Make sure you have the kafka command line tools installed locally.
* Make sure that docker is up and running.


## Building the Pipeline



## Testing the Pipeline

### Bring up the infra with docker

Go into the test folder and use docker-compose to bring up the infra.
```shell
docker-compose up -d
```

### Create Kafka topic and set up producer and consumer

List the topics to make sure that there are no existing topics.
```shell
kafka-topics --list --bootstrap-server localhost:9092
```

Add a topic for us to use.
```shell
kafka-topics --create --topic test.myfirstpipeline --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Verify that this topic shows up when we list the topics.
```shell
kafka-topics --list --bootstrap-server localhost:9092
```

In a shell start a consumer so that we can verify the messages are coming through.
```shell
kafka-console-consumer --topic test.myfirstpipeline --from-beginning --bootstrap-server localhost:9092
```

In another shell start a producer for us to dump messages into.
```shell
kafka-console-producer --topic test.myfirstpipeline  --bootstrap-server localhost:9092
```

In the producer shell put some json to verify this is working.
```shell
{"a": "hello"}
```

Now in a third terminal start the pipeline. Note that I'm in the folder with the pipeline file.
```shell
poetry run python my_first_pipeline.py
```

We should see that the pipeline picks up and proces the messages that was created before the pipeline was started. We should see some logs coming from the pipelin along side this entry coming from our `logger_sink`:
```shell
logger_sink Batch(
	Record(data={'a': 'goodbye'},
       record_uuid=UUID('457afd00-f59e-4615-b3d2-85268c805149'),
       exceptions=[])
)
```
Note: the uuid is generated uniquely for each record so this will be different on each run.

Now in the producer terminal add another message.
```shell
{"a": "hello", "b": "test", "c": 13}
```

We should see the following in the console:
```shell
logger_sink Batch(
	Record(data={'a': 'goodbye', 'b': 'test', 'c': 13},
       record_uuid=UUID('3807ceef-8a95-466d-ab9b-5b6c3e35db33'),
       exceptions=[])
)
```

Note that `a` has had it's value updated but `b` and `c` are left the same.

Let's do one more command.
```shell
{"b": "test", "c": 13}
```
We should see that instead of erroring `a` gets added to the record data.
```shell
logger_sink Batch(
	Record(data={'a': 'goodbye', 'b': 'test', 'c': 13},
       record_uuid=UUID('86ef8667-e052-4ccc-8d34-eb709cbae9f6'),
       exceptions=[])
)
```
It looks like that the case so we can feel good about our pipeline.
Now you can use `Ctrl-c` to stop the pipeline, kafka producer, and the kafka consumer.
Then bring the docker containers down with:
```shell
docker-compose down
```

