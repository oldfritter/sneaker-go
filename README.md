# SneakerWorker for Golang


### Dependencies 依赖
* RabbitMQ

### Usage 使用方法
1.在你的项目中的config目录下创建以下两个文件

```
amqp.yml
workers.yml
```
This is a [example](https://github.com/oldfritter/sneaker-go/blob/master/example/config).

2.workers

[example  示例](https://github.com/oldfritter/sneaker-go/blob/master/example/sneakerWorkers)

3.main

[example  示例](https://github.com/oldfritter/sneaker-go/blob/master/example/main.go)

### workers.yml配置说明
```
---
- name: TreatWorker  # worker的名称
  exchange: sneaker.example.default  # 消息经过的Exchange
  routing_key: sneaker.example.treat  # 消息经过的routing_key
  queue: sneaker.example.treat  # 消息进入的queue
  durable: true
  log: logs/treatWorker.log # 自定义日志文件
  threads: 1  # 并发处理数量
  steps:  # 重试队列的延时配置
    - 5000       # 5 Second
    - 30000      # 30 Second
    - 60000      # 1 Minute
```

### 消息确认与重试

worker 处理成功后才 ack。处理失败的消息按 steps 进入重试队列，超过重试次数后进入 `.failed` 队列；重试拓扑不可用时会 requeue，不会丢掉消息。

| option | 默认 | 说明 |
| --- | --- | --- |
| `ack` | `true` | `false` 时不 ack 处理成功的消息，由 RabbitMQ 重新投递 |
| `retry` | `true` | `false` 时处理失败的消息直接 requeue |
| `steps` | 无 | 每次重试的累计等待时间，如 `5000`、`30000`、`60000`；未配置时失败消息直接进入 `.failed` |
| `retry_ttl` | `24h` | `.retry` 队列的 TTL，即被拒绝消息进入重试链前的等待时间 |
| `max_retry` | steps 档数 | 进入 `.failed` 前的重试次数上限 |
| `requeue` | `true` | 重试拓扑不可用或发布失败时是否 requeue |
| `prefetch` | `0` | 每个消费者的未 ack 上限，`1` 为严格串行 |
| `ack_multiple` | `false` | ack 是否覆盖该 channel 上所有未确认消息，仅在单 goroutine 消费时安全 |

每一个选项都可以写在 `options:` 中，也可以按 worker 覆盖：`options: { "TreatWorker.prefetch": "5" }`。

