package sneaker

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/oldfritter/sneaker-go/utils"
	"github.com/streadway/amqp"
)

type Worker interface {
	GetName() string
	GetExchange() string
	GetRoutingKey() string
	GetQueue() string
	GetDurable() bool
	GetAck() bool
	GetOptions() map[string]string
	GetArguments() map[string]string
	GetDelays() []int32
	GetSteps() []int32
	GetThreads() int
}

func SubscribeMessageByQueue(worker Worker, arguments amqp.Table) (err error) {
	channel, err := utils.RabbitMqConnect.Channel()
	defer channel.Close()
	if err != nil {
		fmt.Println("Channel: ", err)
		return
	}
	channel.QueueDeclare(worker.GetQueue(), worker.GetDurable(), false, false, false, arguments)
	if worker.GetExchange() != "" && worker.GetRoutingKey() != "" {
		channel.ExchangeDeclare(worker.GetExchange(), "topic", worker.GetDurable(), false, false, false, nil)
		channel.QueueBind(worker.GetQueue(), worker.GetRoutingKey(), worker.GetExchange(), false, nil)
		if len(worker.GetSteps()) > 0 {
			channel.ExchangeDeclare(worker.GetQueue()+"-retry", "topic", worker.GetDurable(), false, false, false, nil)
			channel.QueueBind(worker.GetQueue(), "#", worker.GetQueue()+"-retry", false, nil)
		}
	}
	for i, delay := range worker.GetDelays() {
		_, err = channel.QueueDeclare(
			worker.GetQueue()+"-delay-"+strconv.Itoa(i+1), // queue name
			worker.GetDurable(), // durable
			false,               // delete when usused
			false,               // exclusive
			false,               // no-wait
			amqp.Table{"x-dead-letter-exchange": worker.GetQueue() + "-retry", "x-message-ttl": delay}, // arguments
		)
		if err != nil {
			fmt.Println("Queue Declare: ", err)
			return
		}
	}
	for i, step := range worker.GetSteps() {
		_, err = channel.QueueDeclare(
			worker.GetQueue()+"-"+strconv.Itoa(i+1), // queue name
			worker.GetDurable(),                     // durable
			false,                                   // delete when usused
			false,                                   // exclusive
			false,                                   // no-wait
			amqp.Table{"x-dead-letter-exchange": worker.GetQueue() + "-retry", "x-message-ttl": step}, // arguments
		)
		if err != nil {
			fmt.Println("Queue Declare: ", err)
			return
		}
	}
	go func() {
		channel, err := utils.RabbitMqConnect.Channel()
		defer channel.Close()
		if err != nil {
			fmt.Println("Channel: ", err)
			return
		}
		msgs, _ := channel.Consume(
			worker.GetQueue(), // queue
			"",                // consumer
			false,             // auto-ack
			false,             // exclusive
			false,             // no-local
			false,             // no-wait
			nil,               // args
		)
		for d := range msgs {
			response := reflect.ValueOf(worker).MethodByName(worker.GetName()).Call([]reflect.Value{reflect.ValueOf(&d.Body)})
			if !(response[0].String() == "") && !response[1].IsNil() {
				retry(response[0].String(), response[1].Bytes())
			}
			d.Ack(worker.GetAck())
		}
	}()
	return
}

func retry(queueName string, message []byte) (err error) {
	channel, err := utils.RabbitMqConnect.Channel()
	defer channel.Close()
	err = (*channel).Publish(
		"",        // publish to an exchange
		queueName, // routing to 0 or more queues
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            message,
			DeliveryMode:    amqp.Persistent, // amqp.Persistent, amqp.Transient // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
			// a bunch of application/implementation-specific fields
		},
	)
	if err != nil {
		fmt.Println("Channel: ", err)
		return
	}
	return
}
