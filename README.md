# End-to-End Flink Streaming Demo

This project demonstrates an end-to-end Apache Flink streaming application that integrates with Redis, Kafka, and MongoDB. It showcases real-time data processing and stream analytics capabilities using modern data infrastructure.

## Application Flow

The application consists of three main Flink jobs that work together:

1. **Data Generation** ([`DataGenJob`](src/main/java/com/redis/end2end/DataGenJob.java))
   - Generates sample transaction data at a rate of 10 transactions per second
   - Publishes transactions to a Kafka topic

2. **Kafka to Redis Pipeline** ([`FromKafkaToRedisJob`](src/main/java/com/redis/end2end/FromKafkaToRedisJob.java))
   - Consumes transactions from Kafka
   - Stores data in Redis

3. **Data Processing Pipeline** ([`DataProcessingPipelineJob`](src/main/java/com/redis/end2end/DataProcessingPipelineJob.java))
   - Processes the streaming data
   - Performs analytics and transformations
   - Stores results in MongoDB

## Prerequisites

- Java 11 or later
- Docker and Docker Compose
- Gradle

## Project Structure

- `src/main/java/com/redis/end2end/` - Main application code
  - [`DataGenJob.java`](src/main/java/com/redis/end2end/DataGenJob.java) - Data generation job
  - [`DataProcessingPipelineJob.java`](src/main/java/com/redis/end2end/DataProcessingPipelineJob.java) - Main processing pipeline
  - [`FromKafkaToRedisJob.java`](src/main/java/com/redis/end2end/FromKafkaToRedisJob.java) - Kafka to Redis pipeline
- `src/main/resources/` - Configuration files
  - [`end2end.properties`](src/main/resources/end2end.properties) - Application properties
  - [`log4j2.properties`](src/main/resources/log4j2.properties) - Logging configuration

## Infrastructure Components

The application integrates with the following services:

- **Apache Flink** (v1.20.0)
  - JobManager UI available at `http://localhost:8081`
  - Task Manager with 8 task slots
- **Redis Stack** (v7.4.0)
  - Redis server port: 6379
  - RedisInsight UI port: 8001
- **Apache Kafka** (v3.9.0)
  - Broker port: 9092
- **MongoDB** (v8.0.4)
  - MongoDB Express UI port: 8082

## Getting Started

1. Start the infrastructure:
   ```bash
   docker-compose up -d
   ```

2. Build the application:
   ```bash
   ./gradlew shadowJar
   ```

3. Run the end-to-end jobs:
   ```bash
   ./end2endjob.sh
   ```

   This script will:
   - Build the application using Gradle
   - Copy the JAR to the Flink JobManager container
   - Start all three Flink jobs in detached mode:
     1. Data Generation Job
     2. Kafka to Redis Pipeline Job
     3. Data Processing Pipeline Job

4. Monitor the jobs:
   - Check job status in the Flink Dashboard
   - View data in RedisInsight
   - Monitor MongoDB collections through MongoDB Express

## Development

The project uses Gradle for dependency management and build automation. Key dependencies include:

- Flink Core Libraries (v1.20.0)
- Redis Flink Connector (v0.0.2)
- Kafka Flink Connector (v3.4.0)
- MongoDB Flink Connector (v1.2.0)

## Configuration

The application can be configured through:
- [`docker-compose.yml`](docker-compose.yml) for infrastructure settings
- [`build.gradle`](build.gradle) for project dependencies
- Properties files in `src/main/resources/` for application configuration

## Monitoring

- Flink Dashboard: http://localhost:8081
- RedisInsight: http://localhost:8001
- MongoDB Express: http://localhost:8082