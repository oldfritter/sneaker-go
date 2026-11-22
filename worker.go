package sneaker

import (
	"encoding/json"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// workerStateMu serializes the connection and channel pointers of a Worker.
// They are replaced during startup and recovery, and a per-Worker mutex would
// make Worker unsafe to copy after use.
var workerStateMu sync.Mutex

const (
	// defaultRetryTTL is the TTL of the retry queue, the safety net a message
	// rejected by the consumer is dead lettered into. It is the single delay of
	// that hop: messages parked there are redelivered through the stage chain,
	// which applies the configured per-attempt waits.
	defaultRetryTTL = 24 * time.Hour
	// defaultMaxRetry is the retry budget of a worker without retry steps.
	defaultMaxRetry = 3
	// defaultPrefetch bounds the unacknowledged messages held by one consumer
	// channel, so shutdown and redelivery stay bounded as well. 0 keeps the
	// historical behaviour of a server chosen window.
	defaultPrefetch = 0
)

type Worker struct {
	Name         string            `yaml:"name"`
	Exchange     string            `yaml:"exchange"`
	ExchangeType string            `yaml:"exchange_type"`
	RoutingKey   string            `yaml:"routing_key"`
	Queue        string            `yaml:"queue"`
	Log          string            `yaml:"log"`
	Durable      bool              `yaml:"durable"`
	Options      map[string]string `yaml:"options"`
	Arguments    map[string]string `yaml:"arguments"`
	Steps        []string          `yaml:"steps"`
	Threads      int               `yaml:"threads"`

	Ready           bool
	Logger          *log.Logger
	rabbitMqConnect *RabbitMqConnect
	channel         *amqp.Channel
}

// Work 是默认的空实现，具体 worker 应覆盖此方法处理消息。
func (worker *Worker) Work(body *[]byte) (err error) {
	return err
}

// GetName 返回 worker 名称。
func (worker *Worker) GetName() string {
	return worker.Name
}

// GetExchange 返回绑定的 Exchange 名称。
func (worker *Worker) GetExchange() string {
	return worker.Exchange
}

// GetExchangeType 返回 Exchange 类型，未配置时默认为 topic。
func (worker *Worker) GetExchangeType() string {
	if worker.ExchangeType == "" {
		return "topic"
	}
	return worker.ExchangeType
}

// GetRoutingKey 返回队列绑定的 routing key。
func (worker *Worker) GetRoutingKey() string {
	return worker.RoutingKey
}

// GetQueue 返回 worker 消费的队列名称。
func (worker *Worker) GetQueue() string {
	return worker.Queue
}

// GetDelayQueue 返回延迟队列名称。
func (worker *Worker) GetDelayQueue() string {
	return worker.Queue + ".delay"
}

// GetRetryQueue 返回重试队列名称。
func (worker *Worker) GetRetryQueue() string {
	return worker.Queue + ".retry"
}

// GetFailedQueue 返回失败队列名称。
func (worker *Worker) GetFailedQueue() string {
	return worker.Queue + ".failed"
}

// GetLog 返回日志文件路径，未配置时使用默认路径。
func (worker *Worker) GetLog() string {
	if worker.Log != "" {
		return worker.Log
	}
	return DefaultLog
}

// GetLogFolder 返回日志文件所在目录。
func (worker *Worker) GetLogFolder() string {
	return filepath.Dir(worker.GetLog())
}

// GetDurable 返回队列和 Exchange 是否持久化。
func (worker *Worker) GetDurable() bool {
	return worker.Durable
}

// GetOptions 返回 worker 的字符串配置项。
func (worker *Worker) GetOptions() map[string]string {
	return worker.Options
}

// Option 读取字符串配置项；<worker>.<option> 优先于全局 <option>。
func (worker *Worker) Option(name string) (string, bool) {
	options := worker.GetOptions()
	if len(options) == 0 {
		return "", false
	}
	if workerName := worker.GetName(); workerName != "" {
		if value, ok := options[workerName+"."+name]; ok {
			return value, true
		}
	}
	value, ok := options[name]
	return value, ok
}

// OptionInt 读取整数配置项，并允许 s、ms 之类的单位后缀。
func (worker *Worker) OptionInt(name string) (int, bool) {
	value, ok := worker.Option(name)
	if !ok {
		return 0, false
	}
	value = strings.TrimSpace(value)
	digits := value
	if index := strings.IndexFunc(value, func(r rune) bool {
		return r < '0' || r > '9'
	}); index >= 0 {
		suffix := value[index:]
		// A unit suffix such as `s` or `ms` is fine; a sign, a decimal point or
		// another digit means this is not the integer option the caller asked
		// for.
		if strings.ContainsAny(suffix, ".+-0123456789") {
			return 0, false
		}
		digits = value[:index]
	}
	if digits == "" {
		return 0, false
	}
	number, err := strconv.Atoi(digits)
	if err != nil {
		return 0, false
	}
	return number, true
}

// OptionDuration 读取时长配置项；纯数字按毫秒解析。
func (worker *Worker) OptionDuration(name string) (time.Duration, bool) {
	value, ok := worker.Option(name)
	if !ok {
		return 0, false
	}
	return parseDuration(value)
}

// parseDuration 解析时长字符串，纯数字按毫秒处理。
func parseDuration(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if duration, err := time.ParseDuration(value); err == nil {
		return duration, true
	}
	plainNumber := strings.IndexFunc(value, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.'
	}) == -1
	if plainNumber {
		if milliseconds, err := strconv.ParseFloat(value, 64); err == nil {
			return time.Duration(milliseconds * float64(time.Millisecond)), true
		}
	}
	return 0, false
}

// booleanOption 读取布尔配置项，解析失败时返回默认值。
func (worker *Worker) booleanOption(name string, fallback bool) bool {
	value, ok := worker.Option(name)
	if !ok {
		return fallback
	}
	if boolean, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(value))); err == nil {
		return boolean
	}
	return fallback
}

// AckEnabled 返回处理成功后是否 ack，默认 true。
func (worker *Worker) AckEnabled() bool {
	return worker.booleanOption("ack", true)
}

// RequeueEnabled 返回无法重试时是否 requeue，默认 true。
func (worker *Worker) RequeueEnabled() bool {
	return worker.booleanOption("requeue", true)
}

// RetryEnabled 返回失败消息是否进入重试拓扑，默认 true。
func (worker *Worker) RetryEnabled() bool {
	return worker.booleanOption("retry", true)
}

// AckMultiple 返回兼容配置项；实现始终单条 ack。
func (worker *Worker) AckMultiple() bool {
	return worker.booleanOption("ack_multiple", false)
}

// PrefetchCount 返回每个消费者的未 ack 上限。
func (worker *Worker) PrefetchCount() int {
	if count, ok := worker.OptionInt("prefetch"); ok && count >= 0 {
		return count
	}
	return defaultPrefetch
}

// RetryTTL 返回 .retry 队列的 TTL。
func (worker *Worker) RetryTTL() time.Duration {
	if ttl, ok := worker.OptionDuration("retry_ttl"); ok && ttl > 0 {
		return ttl
	}
	return defaultRetryTTL
}

// MaxRetry 返回失败消息进入 .failed 前的最大重试次数。
func (worker *Worker) MaxRetry() int {
	if maximum, ok := worker.OptionInt("max_retry"); ok && maximum >= 0 {
		return maximum
	}
	if steps := len(worker.GetSteps()); steps > 0 {
		return steps
	}
	return defaultMaxRetry
}

// GetArguments 返回队列声明参数。
func (worker *Worker) GetArguments() map[string]string {
	return worker.Arguments
}

// GetSteps 返回重试步骤的累计等待时间。
func (worker *Worker) GetSteps() []string {
	return worker.Steps
}

// GetThreads 返回调用方应启动的消费者数量。
func (worker *Worker) GetThreads() int {
	return worker.Threads
}

// GetRabbitMqConnect 返回当前 RabbitMQ 连接。
func (worker *Worker) GetRabbitMqConnect() *RabbitMqConnect {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	return worker.rabbitMqConnect
}

// SetRabbitMqConnect 替换当前 RabbitMQ 连接。
func (worker *Worker) SetRabbitMqConnect(rabbitMqConnect *RabbitMqConnect) {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	worker.rabbitMqConnect = rabbitMqConnect
}

// GetChannel 返回可用 Channel，必要时自动重新创建。
func (worker *Worker) GetChannel() *amqp.Channel {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()

	if worker.channel != nil && !worker.channel.IsClosed() {
		return worker.channel
	}
	if worker.rabbitMqConnect == nil {
		return nil
	}
	channel, err := worker.rabbitMqConnect.Channel()
	if err != nil {
		worker.channel = nil
		worker.LogError("open channel: ", err)
		return nil
	}
	worker.channel = channel
	return worker.channel
}

// SetChannel 替换 worker 当前使用的 Channel。
func (worker *Worker) SetChannel(channel *amqp.Channel) {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	worker.channel = channel
}

// Perform 将消息序列化为 JSON 并发布到 worker 配置的 Exchange。
func (worker *Worker) Perform(message interface{}) {
	body, err := json.Marshal(message)
	if err != nil {
		worker.LogError("marshal published message: ", err)
		return
	}
	connect := worker.GetRabbitMqConnect()
	if connect == nil {
		worker.LogError("publish message: rabbitmq connection is unavailable")
		return
	}
	if err = connect.PublishMessageWithRouteKey(
		worker.GetExchange(),
		worker.GetRoutingKey(),
		"application/json",
		false,
		false,
		&body,
		amqp.Table{},
		amqp.Persistent,
		"",
	); err != nil {
		worker.LogError("publish message: ", err)
	}
}

// IsChannelClosed 返回当前 Channel 是否为空或已关闭。
func (worker *Worker) IsChannelClosed() bool {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	return worker.channel == nil || worker.channel.IsClosed()
}

// IsReady 返回 worker 是否处于就绪状态。
func (worker *Worker) IsReady() bool {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	return worker.Ready
}

// Start 将 worker 标记为就绪。
func (worker *Worker) Start() {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	worker.Ready = true
}

// Stop 将 worker 标记为未就绪。
func (worker *Worker) Stop() {
	workerStateMu.Lock()
	defer workerStateMu.Unlock()
	worker.Ready = false
}

// Recycle 关闭连接并清空 worker 持有的 Channel。
func (worker *Worker) Recycle() {
	if connect := worker.GetRabbitMqConnect(); connect != nil {
		_ = connect.Close()
	}
	workerStateMu.Lock()
	worker.channel = nil
	workerStateMu.Unlock()
}
