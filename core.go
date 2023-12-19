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
	GetExchangeType() string
	GetRoutingKey() string
	GetQueue() string
	GetDelayQueue() string
	GetRetryQueue() string
	GetFailedQueue() string
	GetLog() string
	GetLogFolder() string
	GetDurable() bool
	GetOptions() map[string]string
	GetArguments() map[string]string
	GetThreads() int
	GetRabbitMqConnect() *RabbitMqConnect
	SetRabbitMqConnect(*RabbitMqConnect)
	GetChannel() *amqp.Channel
	SetChannel(channel *amqp.Channel)

	InitLogger()
	Perform(interface{})
	Retry(d *amqp.Delivery) (err error)

	IsChannelClosed() bool
	IsReady() bool
	Start()
	Stop()
	Recycle()
}
