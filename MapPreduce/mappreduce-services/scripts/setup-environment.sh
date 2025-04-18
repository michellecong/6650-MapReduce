#!/bin/bash

# 环境设置脚本

# 检查 Java 是否已安装
if ! command -v java &> /dev/null; then
    echo "Java is not installed. Please install Java 11 or higher."
    exit 1
fi

# 检查 Maven 是否已安装
if ! command -v mvn &> /dev/null; then
    echo "Maven is not installed. Please install Maven."
    exit 1
fi

# 检查 MySQL 是否已安装
if ! command -v mysql &> /dev/null; then
    echo "MySQL is not installed. Please install MySQL."
    exit 1
fi

# 检查 RabbitMQ 是否已安装
if ! command -v rabbitmqctl &> /dev/null; then
    echo "RabbitMQ is not installed. Would you like to install it? (y/n)"
    read -r install_rabbitmq
    
    if [ "$install_rabbitmq" = "y" ]; then
        # 检测操作系统
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            # 在 Debian/Ubuntu 上安装 RabbitMQ
            if command -v apt-get &> /dev/null; then
                echo "Installing RabbitMQ on Debian/Ubuntu..."
                sudo apt-get update
                sudo apt-get install -y rabbitmq-server
                sudo systemctl enable rabbitmq-server
                sudo systemctl start rabbitmq-server
            # 在 RHEL/CentOS 上安装 RabbitMQ
            elif command -v yum &> /dev/null; then
                echo "Installing RabbitMQ on RHEL/CentOS..."
                sudo yum install -y epel-release
                sudo yum install -y rabbitmq-server
                sudo systemctl enable rabbitmq-server
                sudo systemctl start rabbitmq-server
            else
                echo "Unsupported Linux distribution. Please install RabbitMQ manually."
                exit 1
            fi
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            # 在 macOS 上安装 RabbitMQ
            if command -v brew &> /dev/null; then
                echo "Installing RabbitMQ on macOS using Homebrew..."
                brew install rabbitmq
                brew services start rabbitmq
            else
                echo "Homebrew is not installed. Please install Homebrew or install RabbitMQ manually."
                exit 1
            fi
        else
            echo "Unsupported operating system. Please install RabbitMQ manually."
            exit 1
        fi
    else
        echo "RabbitMQ is required to run the MapReduce system. Please install it manually."
        exit 1
    fi
fi

# 检查 RabbitMQ 是否正在运行
if ! rabbitmqctl status &> /dev/null; then
    echo "RabbitMQ is not running. Starting RabbitMQ..."
    
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo systemctl start rabbitmq-server
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew services start rabbitmq
    else
        echo "Please start RabbitMQ manually."
        exit 1
    fi
fi

# 设置 MySQL 数据库
echo "Setting up MySQL database..."
./scripts/setup-database.sh

# 创建存储目录
mkdir -p mapreduce_data/input
mkdir -p mapreduce_data/intermediate
mkdir -p mapreduce_data/output
mkdir -p logs

# 编译项目
echo "Compiling project..."
mvn clean package

echo "Environment setup completed successfully."
echo "Use start-master.sh to start the Master node."
echo "Use start-worker.sh to start Worker nodes."
echo "Use run-distributed.sh to run a complete MapReduce job."
