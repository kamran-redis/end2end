#!/bin/bash
#./redisha.sh --redis.host redis --redis.port 6379
./gradlew clean shadowJar

docker cp ./build/libs/end2end-all-1.0-SNAPSHOT.jar jobmanager:/opt/flink/examples/streaming
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.chaos.ChaosProducerJob   examples/streaming/end2end-all-1.0-SNAPSHOT.jar  $@
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.chaos.ChaosProcessorJob   examples/streaming/end2end-all-1.0-SNAPSHOT.jar $@
