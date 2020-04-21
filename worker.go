package sneaker

import (
	"fmt"
	"reflect"
	"strconv"

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

type Exception struct {
	Msg string
}

func SubscribeMessageByQueue(RabbitMqConnect *amqp.Connection, worker Worker, arguments amqp.Table) (err error) {
	channel, err := RabbitMqConnect.Channel()
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
			channel.ExchangeDeclare(worker.GetQueue()+".retry", "topic", worker.GetDurable(), false, false, false, nil)
			channel.QueueBind(worker.GetQueue(), "#", worker.GetQueue()+".retry", false, nil)
		}
	}
	for i, delay := range worker.GetDelays() {
		_, err = channel.QueueDeclare(
			worker.GetQueue()+".delay."+strconv.Itoa(i+1), // queue name
			worker.GetDurable(), // durable
			false,               // delete when usused
			false,               // exclusive
			false,               // no-wait
			amqp.Table{"x-dead-letter-exchange": worker.GetQueue() + ".retry", "x-message-ttl": delay}, // arguments
		)
		if err != nil {
			fmt.Println("Queue Declare: ", err)
			return
		}
	}
	for i, step := range worker.GetSteps() {
		_, err = channel.QueueDeclare(
			worker.GetQueue()+".retry."+strconv.Itoa(i+1), // queue name
			worker.GetDurable(), // durable
			false,               // delete when usused
			false,               // exclusive
			false,               // no-wait
			amqp.Table{"x-dead-letter-exchange": worker.GetQueue() + ".retry", "x-message-ttl": step}, // arguments
		)
		if err != nil {
			fmt.Println("Queue Declare: ", err)
			return
		}
	}
	go func() {
		channel, err := RabbitMqConnect.Channel()
		defer channel.Close()
		if err != nil {
			fmt.Println("Channel: ", err)
			return
		}
		msgs, _ := channel.Consume(
			worker.GetQueue(), // queue
			"sneaker-go",      // consumer
			false,             // auto-ack
			false,             // exclusive
			false,             // no-local
			false,             // no-wait
			nil,               // args
		)
		for d := range msgs {
			exception := Exception{}
			response := excute(&worker, &d.Body, &exception)
			if exception.Msg != "" || !response[0].IsNil() {
				d.Headers["err"] = exception.Msg
				if exception.Msg == "" {
					d.Headers["err"] = response[0].String()
				}
				count := 0
				if d.Headers["tryCount"] != nil {
					count = int(d.Headers["tryCount"].(int32))
				}
				if count < len(worker.GetSteps()) {
					retry(RabbitMqConnect, worker.GetQueue(), &d.Body, &d)
				} else {
					logFailedMessageInFailedQueue(RabbitMqConnect, worker.GetQueue(), &d.Body, &d)
				}
			}
			d.Ack(worker.GetAck())
		}
	}()
	return
}

func excute(worker *Worker, body *[]byte, exception *Exception) (response []reflect.Value) {
	defer func(e *Exception) {
		r := recover()
		if r != nil {
			e.Msg = fmt.Sprintf("%v", r)
		}
	}(exception)
	response = reflect.ValueOf(*worker).MethodByName((*worker).GetName()).Call([]reflect.Value{reflect.ValueOf(body)})
	return
}

func retry(RabbitMqConnect *amqp.Connection, queueName string, message *[]byte, d *amqp.Delivery) (err error) {
	count := 1
	if (*d).Headers["tryCount"] != nil {
		count = int((*d).Headers["tryCount"].(int32))
	}
	channel, err := RabbitMqConnect.Channel()
	defer channel.Close()
	err = (*channel).Publish(
		"",        // publish to an exchange
		queueName, // routing to 0 or more queues
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"tryCount": count + 1,
				"err":      (*d).Headers["err"],
			},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            *message,
			DeliveryMode:    amqp.Persistent, // amqp.Persistent, amqp.Transient // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	)
	if err != nil {
		fmt.Println("Channel: ", err)
		return
	}
	return
}

func logFailedMessageInFailedQueue(RabbitMqConnect *amqp.Connection, queueName string, message *[]byte, d *amqp.Delivery) (err error) {
	count := 0
	if (*d).Headers["tryCount"] != nil {
		count = int((*d).Headers["tryCount"].(int32))
	}
	channel, err := RabbitMqConnect.Channel()
	defer channel.Close()
	channel.QueueDeclare(queueName+".faild", true, false, false, false, amqp.Table{})
	err = (*channel).Publish(
		"",                  // publish to an exchange
		queueName+".failed", // routing to 0 or more queues
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"tryCount": count,
				"err":      (*d).Headers["err"],
			},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            *message,
			DeliveryMode:    amqp.Persistent, // amqp.Persistent, amqp.Transient // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	)
	if err != nil {
		fmt.Println("Channel: ", err)
		return
	}
	return
}
