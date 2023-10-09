package sneaker

import (
	"encoding/json"
	"log"
	"regexp"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	Name         string            `yaml:"name"`
	Exchange     string            `yaml:"exchange"`
	ExchangeType string            `yaml:"exchange_type"`
	RoutingKey   string            `yaml:"routing_key"`
	Queue        string            `yaml:"queue"`
	DelayQueue   string            `yaml:"delay_queue"`
	RetryQueue   string            `yaml:"retry_queue"`
	FailedQueue  string            `yaml:"failed_queue"`
	Log          string            `yaml:"log"`
	Durable      bool              `yaml:"durable"`
	Delay        bool              `yaml:"delay"`
	Options      map[string]string `yaml:"options"`
	Arguments    map[string]string `yaml:"arguments"`
	Steps        []string          `yaml:"steps"`
	Threads      int               `yaml:"threads"`
	Ready        bool

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
func (worker *Worker) GetRetryExchange() string {
	return worker.Queue + ".retry"
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
	if worker.DelayQueue != "" {
		return worker.DelayQueue
	}
	return worker.Queue + ".delay"
}
func (worker *Worker) GetRetryQueue() string {
	if worker.RetryQueue != "" {
		return worker.RetryQueue
	}
	return worker.Queue + ".retry"
}
func (worker *Worker) GetFailedQueue() string {
	if worker.FailedQueue != "" {
		return worker.FailedQueue
	}
	return worker.Queue + ".failed"
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
func (worker *Worker) GetDelay() bool {
	return worker.Delay
}
func (worker *Worker) GetOptions() map[string]string {
	return worker.Options
}
func (worker *Worker) GetArguments() map[string]string {
	return worker.Arguments
}
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
	if worker.channel == nil || worker.channel.IsClosed() {
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
