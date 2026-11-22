# 可运行示例

这个目录演示如何用 `sneaker-go` 启动一个 RabbitMQ worker：加载 YAML 配置、声明拓扑、消费消息、按步骤重试，并在连接丢失后恢复消费者。

## 前置条件

- Go 1.23 或更高版本
- 可访问的 RabbitMQ
- 一个可用的 vhost 和账号（示例默认使用 `guest/guest` 与 `/`）

## 快速开始

在仓库根目录执行：

```bash
cd example
go run . -config config
```

默认从当前工作目录的 `config/amqp.yml` 和 `config/workers.yml` 读取配置。`-config` 可以指向其它目录；如果仍在仓库根目录，可执行：

```bash
go run ./example -config ./example/config
```

启动流程：

1. 读取并校验 `amqp.yml`、`workers.yml`
2. 创建进程日志 `logs/workers.log` 和 PID 文件 `pids/workers.pid`
3. 连接 RabbitMQ；失败时每 5 秒重试，收到 SIGINT/SIGTERM 可中断
4. 启动 `workers.yml` 中注册的 worker
5. 每 30 秒检查连接和 channel，必要时重连并恢复消费者
6. 收到退出信号后关闭共享连接并退出

> worker 的 `log` 路径相对于启动时的工作目录。上面的命令会写入 `example/logs/`。

## amqp.yml

```yaml
connect:
  host: 127.0.0.1
  port: "5672"
  username: guest
  password: guest
  vhost: /
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `host` | 是 | RabbitMQ 主机名或 IP；支持 IPv6 |
| `port` | 否 | AMQP 端口，省略时默认 `5672` |
| `username` | 否 | AMQP 用户名 |
| `password` | 否 | AMQP 密码 |
| `vhost` | 否 | vhost，省略时默认 `/`；可配置为 `/sneaker-go` |
| `exchange` | 否 | 预留的额外拓扑配置，当前示例不消费该字段 |

连接 URI 由 `config.Amqp.URL()` 生成，用户名、密码和 vhost 中的 `@`、`:`、`/` 等字符会自动 URL 转义。

## workers.yml

```yaml
---
- name: TreatWorker
  exchange: sneaker.example.default
  exchange_type: topic
  routing_key: sneaker.example.treat
  queue: sneaker.example.treat
  log: logs/treat_worker.log
  durable: true
  threads: 3
  steps: [5000, 30000, 60000]
  options:
    ack: "true"
    retry: "true"
    retry_ttl: 2m
    max_retry: "3"
    requeue: "true"
    prefetch: "1"
```

| 字段 | 说明 |
| --- | --- |
| `name` | worker 名称。示例目前只注册 `TreatWorker` |
| `queue` | 消费队列，必填 |
| `exchange` / `routing_key` | 必须同时配置或同时省略 |
| `exchange_type` | Exchange 类型，默认 `topic` |
| `durable` | 队列和 Exchange 是否持久化 |
| `threads` | 该 worker 启动的消费者数量；`0` 按 `1` 处理 |
| `log` | worker 日志文件；父目录会自动创建 |
| `steps` | 每次重试前的累计等待时间，单位毫秒；未配置时失败消息直接进入 `.failed` |
| `options` | 可选确认、重试和 prefetch 配置，详见根目录 README |

`threads` 决定消费者数量；`prefetch` 决定每个消费者允许持有的未 ack 消息数。两者共同限制并发处理量和内存占用。

## 发送一条测试消息

拓扑由示例启动时自动声明。可以在 RabbitMQ 管理界面发布消息：

- Exchange：`sneaker.example.default`
- Routing key：`sneaker.example.treat`
- Payload：`{"id":1}`

或使用 `rabbitmqadmin`：

```bash
rabbitmqadmin publish \
  exchange=sneaker.example.default \
  routing_key=sneaker.example.treat \
  payload='{"id":1}'
```

成功时可在 `example/logs/treat_worker.log` 看到类似输出：

```text
INFO: TreatWorker payload: {1} , time: 123.456µs
```

发送非法 JSON 会触发重试流程，最终进入 `sneaker.example.treat.failed` 队列。

## 测试

示例目录中的测试不依赖 RabbitMQ：

```bash
go test ./...
go test -race ./...
go test -cover ./...
```

测试覆盖：

- AMQP 配置加载、字段校验、IPv6/特殊字符 URL 转义
- workers.yml 校验与示例配置可加载性
- worker 注册、合法/非法 JSON、nil payload
- 启动连接重试、上下文取消、日志和 PID 文件
- 初始化包对全局连接状态和 dialer 的使用

真正发送消息前，请先启动 RabbitMQ，并按需创建 `vhost` 和账号。

## 自定义 worker

1. 在 `example/sneakerWorkers` 中新增类型并实现 `Work(*[]byte) error`。
2. 在 `TreatWorkers`（或新增的选择函数）中按 `name` 注册实现。
3. 在 `workers.yml` 中添加对应配置。
4. 执行 `go test ./...` 后再启动示例。

`Work` 返回错误时，库会根据 `steps` 进入重试链；因此示例中的 JSON 解析错误也会被当作可重试失败处理。
