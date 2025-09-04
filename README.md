# Pixie Data Service

此项目提供了一个服务，通过调用Pixie API获取数据。该服务允许执行PxL脚本，您可以在请求体中提供脚本内容，也可以引用存储在`scripts`目录中的脚本文件。

## 目录
- [配置](#配置)
- [运行服务](#运行服务)
- [API端点](#api端点)
  - [从请求体执行脚本](#从请求体执行脚本)
  - [从文件执行脚本](#从文件执行脚本)
- [API文档](#api文档)
- [项目结构](#项目结构)
- [日志记录](#日志记录)
- [脚本目录](#脚本目录)

## 配置

应用程序需要在`config.json`中配置以下内容：
- `px_api_key`: 您的Pixie API密钥
- `px_cluster_id`: 您的Pixie集群ID
- `cloud_addr`: Pixie云地址（默认：dev.withpixie.dev:443）

## 运行服务

要运行服务，请执行：
```
go run main.go
```

服务将在端口8080上启动。

## API端点

### 从请求体执行脚本

**端点:** `POST /pixie`

此端点允许通过在请求体中提供脚本内容来执行PxL脚本。

**示例请求:**
```bash
curl -X POST http://localhost:8080/pixie \
  -H "Content-Type: application/json" \
  -d '{"script": "import px\ndf = px.DataFrame(table='http_events', start_time='-1m')\npx.display(df)"}'
```

**示例响应:**
```json
{
  "columns": ["upid", "req_path", "remote_addr", "req_method"],
  "rows": [
    ["12345", "/api/users", "192.168.1.100", "GET"],
    ["67890", "/login", "192.168.1.101", "POST"]
  ],
  "stats": {
    "execution_time_ms": 123,
    "row_count": 2
  }
}
```

### 从文件执行脚本

**端点:** `GET /pixie/script/{scriptName}`

此端点允许执行存储在`scripts`目录中的PxL脚本。您只需要在URL路径中提供不包含`.pxl`扩展名的文件名。

**可选查询参数:**
- `start-time`: 指定查询的开始时间，**格式必须为'-[0-9]+[smh]'**（如'-5m'表示过去5分钟，'-30s'表示过去30秒，'-1h'表示过去1小时等），默认为'-5m'。如果格式不正确，将返回400错误。
- `namespace`: 指定查询的命名空间，默认为'default'。

**示例请求 (默认时间范围):**
```bash
curl http://localhost:8080/pixie/script/conn_status
```

**示例请求 (自定义时间范围):**
```bash
curl http://localhost:8080/pixie/script/conn_status?start-time=-10m
```

这将执行位于`scripts/conn_status.pxl`的脚本文件，并根据指定的时间范围查询数据。

**示例响应:**
```json
{
  "columns": ["time_", "pod", "remote_addr", "bytes_sent"],
  "rows": [
    ["2025-08-22T05:30:00Z", "my-pod-12345", "10.0.0.1:54321", "1024"],
    ["2025-08-22T05:30:01Z", "my-pod-12345", "10.0.0.2:54322", "2048"]
  ],
  "stats": {
    "execution_time_ns": 123456789,
    "records_processed": 250
  }
}
```

## API文档

该服务提供了Swagger UI界面，用于测试和探索API。

- **Swagger UI:** http://localhost:8080/
- **OpenAPI规范:** http://localhost:8080/openapi.json

## 项目结构

项目结构如下：

```
├── .gitignore          # Git忽略文件
├── Dockerfile          # Docker构建文件
├── README.md           # 项目文档
├── config.json         # 应用程序配置
├── go.mod              # Go模块定义
├── go.sum              # Go依赖锁文件
├── index.html          # Swagger UI页面
├── main.go             # 主应用程序代码
├── openapi.json        # OpenAPI规范
├── pixie-service.yaml  # Kubernetes部署文件
└── scripts/            # PxL脚本文件目录
    └── conn_status.pxl # 示例PxL脚本
```

## 日志记录

该服务包含日志功能，将日志输出到控制台和`logs`目录中的文件。

- 日志写入名为`app-YYYY-MM-DD-HH-MM-SS.log`的文件
- 每次服务重启时都会创建一个新的日志文件
- 日志包含有关接收到的请求、查询执行和响应详情的信息
- 错误信息也会被记录下来，以便调试

## 脚本目录

`scripts`目录用于存储可以通过`/pixie/script/{scriptName}`端点执行的PxL脚本文件。

- 所有脚本文件都应具有`.pxl`扩展名
- 调用API时，URL中不需要包含扩展名
- 脚本应使用Pixie PxL语法，并包含`px.display()`调用来返回结果
- 脚本中可以使用`{start_time}`占位标志，该标志会被API请求中的`start-time`查询参数值替换（默认为'-5m'）
- 脚本中可以使用`{namespace}`占位标志，该标志会被API请求中的`namespace`查询参数值替换（默认为'default'）

**示例脚本:**
```python
# 使用{start_time}和{namespace}占位标志的示例
import px

df = px.DataFrame(table='http_events', start_time='{start_time}')
df = df[df.ns == '{namespace}']
px.display(df)
```