package sneaker

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

	InitLogger()
	Perform(interface{})
	SetRabbitMqConnect(*RabbitMqConnect)

	IsReady() bool
	Start()
	Stop()
	Recycle()
}
