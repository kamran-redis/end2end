#!/bin/bash
#./redissample.sh --redis.host redis --jobname helloworld --parallelism 4 --ratelimit 4000
#rate(flink_taskmanager_job_task_operator_helloworld_MY2eventCounter[1m])
#flink_taskmanager_job_task_operator_helloworld_MY1eventTimeLag{quantile="0.99"}
./gradlew clean shadowJar

docker cp ./build/libs/end2end-all-1.0-SNAPSHOT.jar jobmanager:/opt/flink/examples/streaming
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.simple.RedisSample  examples/streaming/end2end-all-1.0-SNAPSHOT.jar $@

