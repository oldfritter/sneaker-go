# SneakerWorker for Golang

基于 RabbitMQ 的 Go worker 框架，提供队列与重试拓扑声明、消息确认、延迟重试、失败队列和并发消费能力。

## 特性

- 一个 worker 对应一个队列，自动声明 Exchange、Queue、`.retry` 和 `.failed` 拓扑
- 处理成功后再 ack；处理失败按 `steps` 进入重试链，耗尽后进入失败队列
- 支持固定延迟、累计退避和自定义 `.retry` TTL
- 同一 Channel 上的 ack/nack/事务串行化，避免并发事务互相回滚
- `SubscribeMessageByQueue` 单消费者串行处理；`SubscribeMessages` 使用有界并发
- 库代码不调用 `log.Fatal`；声明、发布辅助方法和订阅错误通过返回值交给调用方处理，`Perform` 会记录发布错误
- 提供完整可运行示例和无需 RabbitMQ 的单元测试

## 安装

```bash
go get github.com/oldfritter/sneaker-go/v3
```

要求 Go 1.23+。

## 快速开始

完整示例见 [`example/`](example/README.md)。

```bash
cd example
go run . -config config
```

默认使用 `guest/guest` 连接本机 RabbitMQ，并从 `config/amqp.yml`、`config/workers.yml` 读取配置。启动后向 Exchange `sneaker.example.default`、routing key `sneaker.example.treat` 发布 `{"id":1}` 即可看到处理日志。

## 配置

`amqp.yml`：

```yaml
connect:
  host: 127.0.0.1
  port: "5672"
  username: guest
  password: guest
  vhost: /
```

`workers.yml`：

```yaml
---
- name: TreatWorker
  exchange: sneaker.example.default
  exchange_type: topic
  routing_key: sneaker.example.treat
  queue: sneaker.example.treat
  durable: true
  log: logs/treatWorker.log
  threads: 1
  steps:
    - 5000
    - 30000
    - 60000
  options:
    prefetch: "1"
```

| option | 默认 | 说明 |
| --- | --- | --- |
| `ack` | `true` | `false` 时不 ack 处理成功的消息，由 RabbitMQ 重新投递 |
| `retry` | `true` | `false` 时处理失败的消息直接 requeue |
| `steps` | 无 | 每次重试的累计等待时间，如 `5000`、`30000`、`60000`；未配置时失败消息直接进入 `.failed` |
| `retry_ttl` | `24h` | `.retry` 队列的 TTL，即被拒绝消息进入重试链前的等待时间 |
| `max_retry` | steps 档数 | 进入 `.failed` 前的重试次数上限 |
| `requeue` | `true` | 重试拓扑不可用或发布失败时是否 requeue |
| `prefetch` | `0` | 每个消费者的未 ack 上限，`1` 为严格串行 |
| `ack_multiple` | `false` | 兼容配置项；当前实现始终使用单条 ack，避免并发误确认 |

每一个选项都可以写在 `options:` 中，也可以按 worker 覆盖：`options: { "TreatWorker.prefetch": "5" }`。

`threads` 表示示例启动的消费者数量；`prefetch` 表示每个消费者的未 ack 上限。两者需要结合消息处理时间和内存预算配置。

## 确认与重试

1. `Work` 返回 `nil` 且没有 panic：按 `ack` 配置确认消息。
2. `Work` 返回错误或 panic：按 `retry` 配置进入重试链；超过 `max_retry` 后进入 `.failed`。
3. 重试拓扑不可用：按 `requeue` 配置重新投递，避免消息既未处理又未确认。
4. 重试发布使用 Channel 事务；一旦 Channel 进入事务模式，后续 ack/nack 也会提交，避免成功消息长期悬挂。

重连由调用方负责。库会把订阅错误返回给调用方；示例中的监控循环会在连接或 Channel 丢失后重新初始化消费者。

## 并发模型

- `SubscribeMessageByQueue(worker, args)`：声明拓扑并启动一个消费者，单条消息串行处理。需要多消费者时，由调用方启动多个订阅。
- `SubscribeMessages(worker, args)`：声明拓扑并并发处理消息，并发上限由 `GetThreads()` 控制；未配置时回退到 `GOMAXPROCS`。
- 同一 Channel 上的多个订阅共享确认/事务状态，避免事务范围互相干扰。

## 开发与测试

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test -cover ./...
```

根包测试使用 fake channel，不需要 RabbitMQ；`example` 的配置、注册、日志和重试启动测试同样不需要 broker。真实消息收发需要可用的 RabbitMQ。
