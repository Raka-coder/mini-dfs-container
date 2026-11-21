FROM docker.io/library/python:3.12-slim AS builder
WORKDIR /src
COPY requirements.txt requirements-build.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt \
 && pip install --no-cache-dir -r requirements-build.txt
COPY proto ./proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/dfs.proto

FROM docker.io/library/python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY --from=builder /install /usr/local
WORKDIR /app
COPY --from=builder /src/proto ./proto
COPY common ./common
COPY master ./master
COPY datanode ./datanode
COPY client ./client
COPY scripts ./scripts
CMD ["python", "-c", "import sys; sys.exit('specify a command')"]
