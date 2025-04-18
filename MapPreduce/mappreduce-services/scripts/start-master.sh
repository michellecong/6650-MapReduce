#!/bin/bash

# 启动 Master 节点脚本

# 默认配置
RABBITMQ_HOST="localhost"
RABBITMQ_PORT="5672"
RABBITMQ_USER="guest"
RABBITMQ_PASS="guest"
DB_HOST="localhost"
DB_PORT="3306"
DB_NAME="mapreduce"
DB_USER="root"
DB_PASS="password"
BASE_DIR="mapreduce_data"
NUM_REDUCE_TASKS="5"
INPUT_FILE=""

# 解析命令行参数
while getopts "h:p:u:w:H:P:n:U:W:d:r:i:" opt; do
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
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
  esac
done

# 确保日志目录存在
mkdir -p logs

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
     ${INPUT_FILE:+-i "$INPUT_FILE"}
