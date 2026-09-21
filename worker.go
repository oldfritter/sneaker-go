package sneaker

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

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

func (worker *Worker) Work(body *[]byte) (err error) {
	return err
}

func (worker *Worker) GetName() string {
	return worker.Name
}

func (worker *Worker) GetExchange() string {
	return worker.Exchange
}

func (worker *Worker) GetExchangeType() string {
	if worker.ExchangeType == "" {
		worker.ExchangeType = "topic"
	}
	return worker.ExchangeType
}

func (worker *Worker) GetRoutingKey() string {
	return worker.RoutingKey
}

func (worker *Worker) GetQueue() string {
	return worker.Queue
}

func (worker *Worker) GetDelayQueue() string {
	return fmt.Sprintf("%s.delay", worker.Queue)
}

func (worker *Worker) GetRetryQueue() string {
	return fmt.Sprintf("%s.retry", worker.Queue)
}

func (worker *Worker) GetFailedQueue() string {
	return fmt.Sprintf("%s.failed", worker.Queue)
}

func (worker *Worker) GetLog() string {
	if worker.Log != "" {
		return worker.Log
	}
	return DefaultLog
}

func (worker *Worker) GetLogFolder() string {
	re := regexp.MustCompile(`\/.*\.log$`)
	return strings.TrimSuffix(worker.GetLog(), re.FindString(worker.GetLog()))
}

func (worker *Worker) GetDurable() bool {
	return worker.Durable
}

func (worker *Worker) GetOptions() map[string]string {
	return worker.Options
}

// Option reads a string option. Every option can be set in workers.yml under
// `options:` or as a plain field of the worker, and `<name>.<option>` overrides
// `<option>` for a single worker.
func (worker *Worker) Option(name string) (string, bool) {
	for _, key := range []string{worker.GetName() + "." + name, name} {
		if value, ok := worker.GetOptions()[key]; ok {
			return value, true
		}
	}
	return "", false
}

// OptionInt reads an integer option, ignoring a unit suffix such as `3s`.
func (worker *Worker) OptionInt(name string) (int, bool) {
	value, ok := worker.Option(name)
	if !ok {
		return 0, false
	}
	digits := strings.TrimRightFunc(value, func(r rune) bool {
		return r < '0' || r > '9'
	})
	if digits == "" || digits[0] == '-' {
		return 0, false
	}
	number, err := strconv.Atoi(digits)
	if err != nil {
		return 0, false
	}
	return number, true
}

// OptionDuration reads a duration option. A bare number is milliseconds, the
// unit of the historical `steps` configuration, so `3` and `3000` are
// different values.
func (worker *Worker) OptionDuration(name string) (time.Duration, bool) {
	value, ok := worker.Option(name)
	if !ok {
		return 0, false
	}
	return parseDuration(value)
}

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

// AckEnabled reports whether a processed message may be acknowledged. Default
// true, the historical behaviour; `ack: false` leaves a processed message
// unacknowledged, so RabbitMQ redelivers it after the consumer stops.
func (worker *Worker) AckEnabled() bool {
	return worker.booleanOption("ack", true)
}

// RequeueEnabled reports whether an unprocessed message is requeued for
// redelivery when the retry topology is unavailable or the retry publish
// failed. Default true, because a message that is neither acknowledged nor
// requeued is withheld by the server until the consumer goes away.
func (worker *Worker) RequeueEnabled() bool {
	return worker.booleanOption("requeue", true)
}

// RetryEnabled reports whether a failed message is moved to the retry topology
// instead of being requeued to the worker queue.
func (worker *Worker) RetryEnabled() bool {
	return worker.booleanOption("retry", true)
}

// AckMultiple reports whether an ack covers every outstanding delivery of the
// consumer channel. It is only safe while one goroutine consumes the channel,
// so it defaults to false.
func (worker *Worker) AckMultiple() bool {
	return worker.booleanOption("ack_multiple", false)
}

// PrefetchCount is the consumer prefetch, the cap of unacknowledged messages
// one consumer channel holds. `prefetch: 1` gives a strict one message per
// consumer behaviour.
func (worker *Worker) PrefetchCount() int {
	if count, ok := worker.OptionInt("prefetch"); ok && count >= 0 {
		return count
	}
	return defaultPrefetch
}

// RetryTTL is the TTL of the retry queue, the safety net a rejected message is
// dead lettered into. Unlike the stage queues, this wait is a whole hop of its
// own, so it is configured with `retry_ttl`, for example `2m` or `120000`.
func (worker *Worker) RetryTTL() time.Duration {
	if ttl, ok := worker.OptionDuration("retry_ttl"); ok && ttl > 0 {
		return ttl
	}
	return defaultRetryTTL
}

// MaxRetry is the retry budget. A worker with retry steps keeps its historical
// budget, the length of the step list; a worker without steps retries
// defaultMaxRetry times. `max_retry` overrides both.
func (worker *Worker) MaxRetry() int {
	if maximum, ok := worker.OptionInt("max_retry"); ok && maximum >= 0 {
		return maximum
	}
	if steps := len(worker.GetSteps()); steps > 0 {
		return steps
	}
	return defaultMaxRetry
}

func (worker *Worker) GetArguments() map[string]string {
	return worker.Arguments
}

// GetSteps returns the retry step delays. Only the delay of every step is
// used: a rejected message redelivers through the retry topology and is never
// published again, see buildTopology in subscribe.go.
func (worker *Worker) GetSteps() []string {
	return worker.Steps
}

func (worker *Worker) GetThreads() int {
	return worker.Threads
}

func (worker *Worker) GetRabbitMqConnect() *RabbitMqConnect {
	return worker.rabbitMqConnect
}

func (worker *Worker) SetRabbitMqConnect(rabbitMqConnect *RabbitMqConnect) {
	worker.rabbitMqConnect = rabbitMqConnect
}

func (worker *Worker) GetChannel() *amqp.Channel {
	if worker.channel == nil || worker.IsChannelClosed() {
		if worker.rabbitMqConnect != nil {
			worker.channel, _ = worker.rabbitMqConnect.Channel()
		}
	}
	return worker.channel
}

func (worker *Worker) SetChannel(channel *amqp.Channel) {
	worker.channel = channel
}

func (worker *Worker) Perform(message interface{}) {
	b, _ := json.Marshal(&message)
	worker.rabbitMqConnect.PublishMessageWithRouteKey(
		worker.GetExchange(),
		worker.GetRoutingKey(),
		"application/json",
		false,
		false,
		&b,
		amqp.Table{},
		amqp.Persistent,
		"",
	)
}

func (worker *Worker) IsChannelClosed() bool {
	return worker.channel == nil || worker.channel.IsClosed()
}

func (worker *Worker) IsReady() bool {
	return worker.Ready
}

func (worker *Worker) Start() {
	worker.Ready = true
}

func (worker *Worker) Stop() {
	worker.Ready = false
}

func (worker *Worker) Recycle() {
	if !worker.rabbitMqConnect.IsClosed() {
		worker.rabbitMqConnect.Close()
	}
}
