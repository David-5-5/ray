#!/bin/bash
# 编译 gRPC proto 文件生成 Python 代码

set -e

echo "正在编译 scheduler.proto..."

# 检查 grpc_tools 是否安装
if ! python -c "import grpc_tools" 2>/dev/null; then
    echo "错误: 请先安装 grpcio-tools: pip install grpcio-tools"
    exit 1
fi

# 编译 proto 文件
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./proto \
    --grpc_python_out=./proto \
    ./proto/scheduler.proto

echo "编译完成!"
echo "生成的文件:"
ls -la proto/*.py
