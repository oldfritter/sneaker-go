package sneaker

import (
	"sync/atomic"
	"time"

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
	GetSteps() []string
	GetRabbitMqConnect() *RabbitMqConnect
	SetRabbitMqConnect(*RabbitMqConnect)
	GetChannel() *amqp.Channel
	SetChannel(channel *amqp.Channel)

	// Acknowledgement policy. AckEnabled decides whether a processed message is
	// acknowledged, RequeueEnabled whether an unprocessed message is requeued
	// when the retry topology is unavailable, RetryEnabled whether a failed
	// message uses the retry topology. AckMultiple reports the configured request
	// for a cumulative ack, which the sink only honours while a single goroutine
	// settles the channel. PrefetchCount, RetryTTL and MaxRetry shape the
	// consumer window, the retry queue wait and the retry budget.
	AckEnabled() bool
	RequeueEnabled() bool
	RetryEnabled() bool
	AckMultiple() bool
	PrefetchCount() int
	RetryTTL() time.Duration
	MaxRetry() int

	InitLogger()
	Perform(interface{})

	IsChannelClosed() bool
	IsReady() bool
	Start()
	Stop()
	Recycle()
}

// consumerSequence numbers consumer tags. RabbitMQ rejects a consumer tag that
// is already in use on a channel, and every consumer of a worker shares the
// queue name, so the tag cannot be the queue name alone.
var consumerSequence uint64

func atomicNextConsumerSequence() uint64 {
	return atomic.AddUint64(&consumerSequence, 1)
}
