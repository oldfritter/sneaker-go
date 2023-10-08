package sneaker

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Exception struct {
	Msg string
}

type WorkerI interface {
	Work(*[]byte) error
	GetName() string
	GetExchange() string
	GetRetryExchange() string
	GetExchangeType() string
	GetRoutingKey() string
	GetQueue() string
	GetDelayQueue() string
	GetRetryQueue() string
	GetFailedQueue() string
	GetLog() string
	GetLogFolder() string
	GetDurable() bool
	GetDelay() bool
	GetOptions() map[string]string
	GetArguments() map[string]string
	GetSteps() []string
	GetThreads() int
	GetRabbitMqConnect() *RabbitMqConnect
	SetRabbitMqConnect(*RabbitMqConnect)
	GetChannel() *amqp.Channel
	SetChannel(channel *amqp.Channel)

	InitLogger()
	Perform(interface{})

	IsReady() bool
	Start()
	Stop()
	Recycle()
}
