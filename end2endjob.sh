#!/bin/bash

./gradlew clean shadowJar

docker cp ./build/libs/end2end-all-1.0-SNAPSHOT.jar jobmanager:/opt/flink/examples/streaming
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.DataGenJob  examples/streaming/end2end-all-1.0-SNAPSHOT.jar
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.FromKafkaToRedisJob   examples/streaming/end2end-all-1.0-SNAPSHOT.jar
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.DataProcessingPipelineJob   examples/streaming/end2end-all-1.0-SNAPSHOT.jar
