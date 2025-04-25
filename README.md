# MapReduce Word Count System

A scalable word counting solution with both distributed and single-node implementations. This system demonstrates MapReduce architecture principles for processing large text datasets.

## Overview

This project provides a complete word counting system with multiple components:

1. **MapReduce Web** - Web interface and API gateway for job submission and result retrieval
2. **MapReduce Service** - Distributed processing engine with master and worker nodes
3. **SingleNode WebApp** - Simplified non-distributed implementation with API compatibility
4. **Load Tester** - Performance testing framework for system evaluation

The system allows users to submit text content, process it to count word frequencies, and retrieve the results through a RESTful API.

## Components

### MapReduce Web

The web component provides a RESTful API for:

- Submitting text content for word counting
- Checking job status
- Retrieving word count results

**Key Features:**

- RESTful API with JSON responses
- Azure Blob Storage integration
- MySQL database for job metadata and results

### MapReduce Service

The distributed processing engine includes:

**Master Node:**

- Job scheduling and distribution
- Task management and monitoring
- Worker coordination
- Fault tolerance handling

**Worker Nodes:**

- Execute Map and Reduce tasks
- Process data chunks in parallel
- Report results to the master
- Send periodic heartbeats for health monitoring

**Features:**

- Horizontal scalability with dynamic worker addition
- Fault tolerance through task rescheduling
- RabbitMQ-based message passing
- Hybrid storage supporting local files and Azure Blob Storage

### SingleNode WebApp

A simplified implementation that:

- Provides the same API as the distributed version
- Processes data in a single thread
- Supports development, testing, and small workloads

**Benefits:**

- Simplified deployment for small workloads
- Development and testing environment
- API compatibility with the distributed version

### Load Tester

A comprehensive testing framework to:

- Evaluate system performance under various loads
- Verify result accuracy
- Generate test reports

**Features:**

- Concurrent user simulation
- Performance metrics collection (latency, throughput)
- Result accuracy verification
- CSV report generation

## API Endpoints

### Job Submission

```
POST /api/jobs
```

Request body:

```json
{
  "text": "Content to process",
  "fileName": "input.txt",
  "numReduceTasks": 5,
  "useBlob": true
}
```

### Job Status

```
GET /api/jobs/{jobId}
```

### Word Count Results

```
GET /api/jobs/{jobId}/wordcount
```

### Execution Time

```
GET /api/jobs/{jobId}/execution-time
```

## Technology Stack

- **Languages:** Java
- **Web Framework:** Java Servlets
- **Database:** MySQL with HikariCP connection pooling
- **Messaging:** RabbitMQ
- **Storage:** Azure Blob Storage
- **Configuration:** Properties files
- **Logging:** Log4j2

## Getting Started

### Prerequisites

- Java 11 or higher
- MySQL 5.7+
- RabbitMQ 3.8+
- Maven or Gradle

### Configuration

Edit the `application.properties` file:

```properties
# Database
db.host=localhost
db.port=3306
db.name=mapreduce
db.user=admin
db.password=admin

# RabbitMQ
rabbitmq.host=localhost
rabbitmq.port=5672
rabbitmq.username=guest
rabbitmq.password=guest

# Azure Blob Storage
azure.storage.connectionString=your-connection-string
azure.storage.containerName=mapreduce
```

### Running the Components

**MapReduce Web:**

```bash
java -jar mapreduce-web.jar
```

**MapReduce Master:**

```bash
java -jar mapreduce-master.jar
```

**MapReduce Worker:**

```bash
java -jar mapreduce-worker.jar
```

**SingleNode WebApp:**

```bash
java -jar singlenode-webapp.jar
```

**Load Tester:**

```bash
java -jar loadtester.jar http://localhost:8080 10 5 ./test-files
```

Parameters:

- Server URL
- Number of concurrent users
- Requests per user
- Test files directory (optional)

## Performance Comparison

| Implementation          | Small Text (1KB) | Medium Text (100KB) | Large Text (1MB) |
| ----------------------- | ---------------- | ------------------- | ---------------- |
| SingleNode              | ~200ms           | ~1s                 | ~5s              |
| Distributed (2 workers) | ~500ms           | ~800ms              | ~3s              |
| Distributed (5 workers) | ~800ms           | ~500ms              | ~1.5s            |

_Note: Performance metrics are approximate and depend on hardware configuration._

## License

This project is licensed under the MIT License - see the LICENSE file for details.
