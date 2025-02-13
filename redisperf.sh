#!/bin/bash
#./redisperf.sh --redis.host redis --jobname helloworld --parallelism 4 --ratelimit 4000
#./redisperf.sh --redis.host localhost --redis.port 6379 --jobname redisflinkperf --parallelism 32 --ratelimit 30000 --payloadsize 100 --redis.user test --redis.password password
#rate(flink_taskmanager_job_task_operator_redisflinkperf_eventCounter[1m])
#sum(rate(flink_taskmanager_job_task_operator_redisflinkperf_eventCounter[1m]))
#flink_taskmanager_job_task_operator_redisflinkperf_eventTimeLag{quantile="0.99"}
#avg(flink_taskmanager_job_task_operator_redisflinkperf_eventTimeLag{quantile="0.99"})
./gradlew clean shadowJar

docker cp ./build/libs/end2end-all-1.0-SNAPSHOT.jar jobmanager:/opt/flink/examples/streaming
docker exec -it jobmanager ./bin/flink run -d  -c com.redis.end2end.simple.RedisSample  examples/streaming/end2end-all-1.0-SNAPSHOT.jar $@

