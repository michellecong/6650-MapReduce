# 6650-MapReduce

This application is a prototype that simulates a distributed word count system using RabbitMQ and a MapReduce pattern. The code has been updated to prepare for simulations that more closely resemble actual displays, and uses a configuration file for all settings.

## Prerequisites

- Java JDK 8 or higher
- MySQL database
- RabbitMQ server
- Maven (optional, for dependency management)

## Setting Up

### 1. Database Setup

Create a MySQL database and user:

```sql
CREATE DATABASE wordcount;
CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON wordcount.* TO 'user'@'localhost';
FLUSH PRIVILEGES;
```

### 2. Configuration

The application uses a properties file for configuration. A default `config.properties` file is provided, but you can create your own and specify it when running the application.

Key configuration sections:

- Queue names
- Database connection
- RabbitMQ connection
- Batch processing settings
- Worker behaviors
- Result output configuration

### 3. Required Dependencies

- RabbitMQ Java Client
- HikariCP (connection pool)
- MySQL Connector/J

## Running The Application

### Specifying Configuration

You can specify a custom configuration file using the `--config` or `-c` option:

```bash
java -cp wordcount.jar com.wordcount.WordCount --config /path/to/your/config.properties [command]
```

If you don't specify a configuration file, the application will look for `config.properties` in the current directory.

### Available Commands

1. **Start a Map Worker**

   ```bash
   java -cp wordcount.jar com.wordcount.WordCount map
   ```

2. **Start a Reduce Worker**

   ```bash
   java -cp wordcount.jar com.wordcount.WordCount reduce
   ```

3. **Start the Result Collector**

   ```bash
   java -cp wordcount.jar com.wordcount.WordCount result
   ```

4. **Submit a File for Processing**

   ```bash
   java -cp wordcount.jar com.wordcount.WordCount submit /path/to/textfile.txt
   ```

5. **Monitor System Status**

   ```bash
   java -cp wordcount.jar com.wordcount.WordCount monitor
   ```

### Setting Up Multiple Workers

For better performance, you can run multiple Map and Reduce workers:

```bash
# In terminal 1
java -cp wordcount.jar com.wordcount.WordCount map

# In terminal 2
java -cp wordcount.jar com.wordcount.WordCount map

# In terminal 3
java -cp wordcount.jar com.wordcount.WordCount reduce

# In terminal 4
java -cp wordcount.jar com.wordcount.WordCount reduce

# In terminal 5
java -cp wordcount.jar com.wordcount.WordCount result

# In terminal 6
java -cp wordcount.jar com.wordcount.WordCount monitor
```

## Using Environment Variables with Configuration

You can use environment variables to override configuration without changing the file. To do this, set environment variables with the same names as the properties, but in uppercase with dots replaced by underscores.

For example:

- To override `db.url`, set the environment variable `DB_URL`
- To override `rabbit.host`, set the environment variable `RABBIT_HOST`

Example:

```bash
export DB_URL=jdbc:mysql://production-db:3306/wordcount
export DB_USER=prod_user
export DB_PASSWORD=secure_password
java -cp wordcount.jar com.wordcount.WordCount map
```

## Docker Deployment

For Docker deployment, you can mount a custom configuration file and set environment variables:

```bash
docker run -v /path/to/config.properties:/app/config.properties \
  -e RABBIT_HOST=rabbitmq-container \
  -e DB_URL=jdbc:mysql://mysql-container:3306/wordcount \
  wordcount-image map
```

## Troubleshooting

1. **Database Connection Issues**

   - Verify MySQL is running
   - Check database credentials in config file
   - Ensure the database exists

2. **RabbitMQ Connection Issues**

   - Verify RabbitMQ is running
   - Check connection details in config file

3. **Worker Not Processing**
   - Check worker heartbeats in monitor
   - Verify queue names are consistent

## Monitoring and Management

Use the `monitor` command to track:

- Active workers
- Running tasks and progress
- Completed tasks

Results are saved in the directory specified by `result.output.dir` in the configuration (default is current directory).
