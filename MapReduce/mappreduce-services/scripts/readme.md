# 基于 RabbitMQ 和 MySQL 的分布式 MapReduce 词频统计系统

这个项目实现了一个完整的分布式 MapReduce 系统，用于计算文本文件中单词的出现频率。该系统使用 RabbitMQ 进行任务分配和消息通信，使用 MySQL 数据库存储任务状态和最终的词频统计结果。

## 系统架构

系统主要包含以下组件：

1. **Master 节点**：负责任务分割、分配和监控
2. **Worker 节点**：执行 Map 和 Reduce 任务
3. **RabbitMQ**：提供消息队列服务，用于任务分配和状态通信
4. **MySQL**：存储任务状态和词频统计结果
5. **文件系统**：存储输入、中间结果和最终输出

### 数据流

1. **输入分割**：Master 将输入文件分割成多个块
2. **Map 阶段**：Worker 处理文本块，统计单词频率，按分区存储结果
3. **Shuffle 阶段**：系统将相同单词的中间结果分配到对应的 Reduce 任务
4. **Reduce 阶段**：Worker 合并相同单词的计数
5. **结果存储**：最终结果保存在 MySQL 数据库中和输出文件中

### 系统流程

Master 将输入文件分割成多个块。
Master 创建 Map 任务并发送到 RabbitMQ 队列。
Worker 从队列获取 Map 任务并执行。
Worker 将 Map 任务结果写入中间文件并发送结果通知。
Master 监控所有 Map 任务的完成情况。
当所有 Map 任务完成后，Master 创建 Reduce 任务。
Worker 从队列获取 Reduce 任务并执行。
Worker 将 Reduce 任务结果写入输出文件并加载到数据库。
Master 监控所有 Reduce 任务的完成情况。
当所有任务完成后，作业完成，最终结果可在数据库和输出文件中获取。
## 使用说明

### 先决条件

- Java 11 或更高版本
- Maven 3.6 或更高版本
- MySQL 5.7 或更高版本
- RabbitMQ 3.8 或更高版本

### 快速入门

1. 首先，设置环境：

```bash
chmod +x setup-environment.sh
./setup-environment.sh
```

2. 准备一个输入文本文件，例如：

```bash
echo "Hello world MapReduce is a distributed computing model MapReduce processes data in parallel" > input.txt
```

3. 运行 MapReduce 作业：

```bash
chmod +x run-distributed.sh
./run-distributed.sh -i input.txt
```

这将在本地启动一个 Master 节点和多个 Worker 节点，处理输入文件，并在数据库中存储词频统计结果。

4. 查看结果：

```bash
mysql -u root -ppassword mapreduce -e "SELECT word, count FROM word_counts ORDER BY count DESC LIMIT 10;"
```

### 手动启动组件

如果需要更灵活的控制，可以手动启动各个组件：

1. 启动 Master 节点：

```bash
chmod +x start-master.sh
./start-master.sh -i input.txt
```

2. 启动 Worker 节点：

```bash
chmod +x start-worker.sh
./start-worker.sh
```

### 分布式模式

要在多台机器上运行系统，使用以下命令：

```bash
./run-distributed.sh -i input.txt -L worker1.example.com,worker2.example.com,worker3.example.com
```

确保所有机器都可以通过 SSH 访问，并且都安装了 Java 和必要的依赖项。

## 配置参数

以下是可用的配置参数：

| 参数 | 描述 | 默认值 |
|-----|------|-------|
| `-h` | RabbitMQ 主机 | localhost |
| `-p` | RabbitMQ 端口 | 5672 |
| `-u` | RabbitMQ 用户名 | guest |
| `-w` | RabbitMQ 密码 | guest |
| `-H` | MySQL 主机 | localhost |
| `-P` | MySQL 端口 | 3306 |
| `-n` | MySQL 数据库名称 | mapreduce |
| `-U` | MySQL 用户名 | root |
| `-W` | MySQL 密码 | password |
| `-d` | 基础存储目录 | mapreduce_data |
| `-r` | Reduce 任务数量 | 5 |
| `-i` | 输入文件路径 | (必需) |
| `-N` | 本地 Worker 进程数量 | 3 |
| `-L` | Worker 主机列表 (逗号分隔) | (无，本地模式) |

## 系统特点

1. **容错性**：
   - Worker 心跳检测机制
   - 失败任务自动重试
   - 长时间运行任务检测和处理

2. **可扩展性**：
   - 可以动态添加 Worker 节点
   - 支持大规模文本处理

3. **状态持久化**：
   - 使用 MySQL 数据库持久化所有状态
   - 系统崩溃后可以恢复任务

4. **结果存储**：
   - 词频统计结果保存在数据库中
   - 支持高效查询和分析

## 数据库模式

系统使用以下 MySQL 表：

1. `jobs` - 存储作业信息
2. `tasks` - 存储任务信息
3. `map_outputs` - 存储 Map 输出文件路径
4. `workers` - 存储 Worker 节点状态
5. `word_counts` - 存储词频统计结果

## 扩展与改进

可以考虑以下扩展和改进：

1. 添加 Web 界面监控系统状态
2. 支持更多类型的 MapReduce 作业
3. 添加任务优先级支持
4. 实现更智能的负载均衡策略
5. 改进数据局部性优化

## 故障排除

如果遇到问题，请检查：

1. RabbitMQ 和 MySQL 服务是否正在运行
2. 数据库连接参数是否正确
3. 所有机器是否可以通过网络互相访问
4. Java 版本是否兼容
5. 查看日志文件中的错误信息

日志文件位于 `logs/` 目录。

## 贡献

欢迎提交 Pull Request 和问题报告。请确保遵循代码风格并添加适当的测试。

基于 RabbitMQ 和 MySQL 的 MapReduce 项目结构和运行指南
### 项目结构
本项目的目录结构组织如下：
mapreduce-mysql/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── mapreduce/
│       │           ├── common/       # 公共类和枚举
│       │           │   ├── Constants.java
│       │           │   ├── JobStatus.java
│       │           │   ├── KeyValue.java
│       │           │   ├── Task.java
│       │           │   ├── TaskStatus.java
│       │           │   └── TaskType.java
│       │           ├── db/           # 数据库访问层
│       │           │   ├── DatabaseConfig.java
│       │           │   ├── JobDao.java
│       │           │   ├── MapOutputDao.java
│       │           │   ├── TaskDao.java
│       │           │   ├── WordCountDao.java
│       │           │   └── WorkerDao.java
│       │           ├── master/       # Master 节点代码
│       │           │   ├── FilePartitioner.java
│       │           │   ├── Master.java
│       │           │   ├── TaskMonitor.java
│       │           │   └── TaskScheduler.java
│       │           ├── messaging/    # 消息处理
│       │           │   ├── MessageConsumer.java
│       │           │   ├── MessageProducer.java
│       │           │   └── RabbitMQClient.java
│       │           ├── storage/      # 存储管理
│       │           │   └── StorageManager.java
│       │           └── worker/       # Worker 节点代码
│       │               ├── MapWorker.java
│       │               ├── ReduceWorker.java
│       │               └── Worker.java
│       └── resources/
│           └── log4j2.xml           # 日志配置
├── scripts/                         # 启动和运行脚本
│   ├── run-distributed.sh
│   ├── setup-database.sh
│   ├── setup-environment.sh
│   ├── start-master.sh
│   └── start-worker.sh
├── pom.xml                          # Maven 配置
└── README.md                        # 项目说明

### 系统执行流程举例
假设我们有一个包含以下内容的 input.txt 文件：
Hello world MapReduce is a programming model for large data
MapReduce simplifies data processing on large clusters
当执行 ./scripts/run-distributed.sh -i input.txt 时：

初始化阶段：

Master 创建一个作业 ID（如 "job_1234"）
在 jobs 表中插入一条记录，状态为 RUNNING


Map 阶段：

由于文件较小，只创建一个 Map 任务
在 tasks 表中插入一条 MAP 类型的任务记录
将任务发送到 RabbitMQ 的 map_tasks_queue
Worker 从队列获取任务并执行
对每个单词计数：{"Hello": 1, "world": 1, "MapReduce": 2, ...}
按照哈希值将单词分到不同分区，例如：

分区 0：{"Hello": 1, "data": 1, ...}
分区 1：{"world": 1, "large": 2, ...}
分区 2：{"MapReduce": 2, "is": 1, ...}


将分区写入中间文件
在 map_outputs 表中记录每个分区文件
将完成通知发送到 map_results_queue


Reduce 阶段：

Master 收到 Map 任务完成通知，创建 Reduce 任务（默认 5 个）
在 tasks 表中插入 5 条 REDUCE 类型的任务记录
将任务发送到 reduce_tasks_queue
Worker 从队列获取任务并执行
对于分区 2，合并所有 MapReduce 的计数为 2
将结果写入输出文件
将词频结果保存到 word_counts 表：
| id | job_id  | word      | count |
|----|---------|-----------|-------|
| 1  | job_1234| Hello     | 1     |
| 2  | job_1234| world     | 1     |
| 3  | job_1234| MapReduce | 2     |
| 4  | job_1234| is        | 1     |
| ...| ...     | ...       | ...   |

将完成通知发送到 reduce_results_queue


完成阶段：

Master 检测到所有任务已完成
合并所有输出文件为最终结果
更新 jobs 表中的作业状态为 COMPLETED



这个系统的核心设计亮点在于：

分布式任务调度：通过 RabbitMQ 实现可靠的任务分发
状态持久化：通过 MySQL 数据库存储所有状态，提高系统容错性
细粒度任务跟踪：详细记录每个任务的状态和输出
容错机制：Worker 心跳检测和任务重试机制保证系统可靠性
模块化设计：各组件职责明确，易于扩展和维护

希望这个详细解释能帮助您理解系统的核心设计和工作原理。如果有特定部分需要更深入了解，请告诉我。