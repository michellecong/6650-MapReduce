#!/bin/bash

# 在多台机器上分布式运行 MapReduce 系统

# 默认配置
RABBITMQ_HOST="localhost"
RABBITMQ_PORT="5672"
RABBITMQ_USER="guest"
RABBITMQ_PASS="guest"
DB_HOST="localhost"
DB_PORT="3306"
DB_NAME="mapreduce"
DB_USER="admin"
DB_PASS="admin"
BASE_DIR="mapreduce_data"
NUM_REDUCE_TASKS="10"
INPUT_FILE=""
NUM_WORKERS=5
WORKER_HOSTS=()

# 解析命令行参数
while getopts "h:p:u:w:H:P:n:U:W:d:r:i:N:L:" opt; do
  case $opt in
    h) RABBITMQ_HOST="$OPTARG" ;;
    p) RABBITMQ_PORT="$OPTARG" ;;
    u) RABBITMQ_USER="$OPTARG" ;;
    w) RABBITMQ_PASS="$OPTARG" ;;
    H) DB_HOST="$OPTARG" ;;
    P) DB_PORT="$OPTARG" ;;
    n) DB_NAME="$OPTARG" ;;
    U) DB_USER="$OPTARG" ;;
    W) DB_PASS="$OPTARG" ;;
    d) BASE_DIR="$OPTARG" ;;
    r) NUM_REDUCE_TASKS="$OPTARG" ;;
    i) INPUT_FILE="$OPTARG" ;;
    N) NUM_WORKERS="$OPTARG" ;;
    L) IFS=',' read -r -a WORKER_HOSTS <<< "$OPTARG" ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
  esac
done

# 检查输入文件是否存在
if [ -z "$INPUT_FILE" ]; then
    echo "Please specify an input file using -i option"
    exit 1
fi

if [ ! -f "$INPUT_FILE" ]; then
    echo "Input file does not exist: $INPUT_FILE"
    exit 1
fi

# 如果没有指定 Worker 主机，使用本地多进程模式
if [ ${#WORKER_HOSTS[@]} -eq 0 ]; then
    echo "No worker hosts specified, running in local multi-process mode."
    
    # 启动 Master 节点
    echo "Starting Master node..."
    java -cp target/mapreduce-mysql-1.0-SNAPSHOT.jar \
         com.mapreduce.master.Master \
         -rh "$RABBITMQ_HOST" \
         -rp "$RABBITMQ_PORT" \
         -ru "$RABBITMQ_USER" \
         -rpw "$RABBITMQ_PASS" \
         -dh "$DB_HOST" \
         -dp "$DB_PORT" \
         -dn "$DB_NAME" \
         -du "$DB_USER" \
         -dpw "$DB_PASS" \
         -d "$BASE_DIR" \
         -r "$NUM_REDUCE_TASKS" \
         -i "$INPUT_FILE" &
    
    MASTER_PID=$!
    echo "Master started with PID: $MASTER_PID"
    
    # 等待 Master 启动完成
    sleep 5
    
    # 启动 Worker 节点
    WORKER_PIDS=()
    for ((i=1; i<=NUM_WORKERS; i++)); do
        echo "Starting Worker $i..."
        java -cp target/mapreduce-mysql-1.0-SNAPSHOT.jar \
             com.mapreduce.worker.Worker \
             -rh "$RABBITMQ_HOST" \
             -rp "$RABBITMQ_PORT" \
             -ru "$RABBITMQ_USER" \
             -rpw "$RABBITMQ_PASS" \
             -dh "$DB_HOST" \
             -dp "$DB_PORT" \
             -dn "$DB_NAME" \
             -du "$DB_USER" \
             -dpw "$DB_PASS" \
             -d "${BASE_DIR}_worker_${i}" \
             -r "$NUM_REDUCE_TASKS" &
        
        WORKER_PID=$!
        WORKER_PIDS+=($WORKER_PID)
        echo "Worker $i started with PID: $WORKER_PID"
        
        # 等待 Worker 启动完成
        sleep 2
    done
    
    # 等待 Master 完成
    echo "Waiting for MapReduce job to complete..."
    wait $MASTER_PID
    
    # 任务完成后，杀死所有 Worker 进程
    for pid in "${WORKER_PIDS[@]}"; do
        echo "Stopping Worker with PID: $pid"
        kill $pid
    done
    
else
    # 分布式模式：在多台机器上运行
    echo "Running in distributed mode across ${#WORKER_HOSTS[@]} machines."
    
    # 检查 SSH 是否可用
    if ! command -v ssh &> /dev/null; then
        echo "SSH is not installed. It is required for distributed mode."
        exit 1
    fi
    
    # 启动 Master 节点（本地）
    echo "Starting Master node locally..."
    java -cp target/mapreduce-mysql-1.0-SNAPSHOT.jar \
         com.mapreduce.master.Master \
         -rh "$RABBITMQ_HOST" \
         -rp "$RABBITMQ_PORT" \
         -ru "$RABBITMQ_USER" \
         -rpw "$RABBITMQ_PASS" \
         -dh "$DB_HOST" \
         -dp "$DB_PORT" \
         -dn "$DB_NAME" \
         -du "$DB_USER" \
         -dpw "$DB_PASS" \
         -d "$BASE_DIR" \
         -r "$NUM_REDUCE_TASKS" \
         -i "$INPUT_FILE" &
    
    MASTER_PID=$!
    echo "Master started with PID: $MASTER_PID"
    
    # 等待 Master 启动完成
    sleep 5
    
    # 在远程机器上启动 Worker 节点
    worker_index=0
    for host in "${WORKER_HOSTS[@]}"; do
        worker_index=$((worker_index + 1))
        echo "Starting Worker on $host..."
        
        # 使用 SSH 在远程机器上启动 Worker
        ssh "$host" "cd $(pwd) && java -cp target/mapreduce-mysql-1.0-SNAPSHOT.jar \
                     com.mapreduce.worker.Worker \
                     -rh '$RABBITMQ_HOST' \
                     -rp '$RABBITMQ_PORT' \
                     -ru '$RABBITMQ_USER' \
                     -rpw '$RABBITMQ_PASS' \
                     -dh '$DB_HOST' \
                     -dp '$DB_PORT' \
                     -dn '$DB_NAME' \
                     -du '$DB_USER' \
                     -dpw '$DB_PASS' \
                     -d '${BASE_DIR}_worker_${worker_index}' \
                     -r '$NUM_REDUCE_TASKS'" &
        
        echo "Worker started on $host"
        sleep 2
    done
    
    # 等待 Master 完成
    echo "Waiting for MapReduce job to complete..."
    wait $MASTER_PID
    
    # 任务完成后，停止远程 Worker 进程
    for host in "${WORKER_HOSTS[@]}"; do
        echo "Stopping Worker on $host..."
        ssh "$host" "pkill -f 'java -cp target/mapreduce-mysql-1.0-SNAPSHOT.jar com.mapreduce.worker.Worker'"
    done
fi

echo "MapReduce job completed."
echo "Check results in the database and output directory: $BASE_DIR/output/"

# 显示结果
echo "Top 20 words with highest frequency:"
mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" $DB_NAME -e "SELECT word, count FROM word_counts ORDER BY count DESC LIMIT 20;"
