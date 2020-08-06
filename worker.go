package sneaker

import (
	"fmt"
	"log"
	"reflect"
	"strconv"

	"github.com/streadway/amqp"
)

type Worker interface {
	GetName() string
	GetExchange() string
	GetExchangeType() string
	GetRoutingKey() string
	GetQueue() string
	GetDurable() bool
	GetDelay() bool
	GetOptions() map[string]string
	GetArguments() map[string]string
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
		log.Println("Channel: ", err)
		return
	}
	_, err = channel.QueueDeclare(worker.GetQueue(), worker.GetDurable(), false, false, false, arguments)
	if err != nil {
		log.Println("Queue ", worker.GetQueue(), " declare error: ", err)
		return
	}
	if worker.GetExchange() != "" && worker.GetRoutingKey() != "" {
		err = channel.ExchangeDeclare(worker.GetExchange(), worker.GetExchangeType(), worker.GetDurable(), false, false, false, nil)
		if err != nil {
			log.Println("Exchange ", worker.GetExchange(), " declare error: ", err)
			return
		}
		err = channel.QueueBind(worker.GetQueue(), worker.GetRoutingKey(), worker.GetExchange(), false, nil)
		if err != nil {
			log.Println("Queue ", worker.GetQueue(), " bind error: ", err)
			return
		}
		if len(worker.GetSteps()) > 0 {
			err = channel.ExchangeDeclare(worker.GetQueue()+".retry", "topic", worker.GetDurable(), false, false, false, nil)
			if err != nil {
				log.Println("Exchange ", worker.GetQueue()+".retry", " declare error: ", err)
				return
			}
			err = channel.QueueBind(worker.GetQueue(), "#", worker.GetQueue()+".retry", false, nil)
			if err != nil {
				log.Println("Queue ", worker.GetQueue(), " bind error: ", err)
				return
			}
		}
	}
	if worker.GetDelay() {
		_, err = channel.QueueDeclare(worker.GetQueue()+".delay", worker.GetDurable(), false, false, false, amqp.Table{"x-dead-letter-exchange": worker.GetQueue() + ".retry"})
		if err != nil {
			log.Println("Queue ", worker.GetQueue()+".delay declare error : ", err)
			return
		}
	}
	for i, step := range worker.GetSteps() {
		_, err = channel.QueueDeclare(worker.GetQueue()+".retry."+strconv.Itoa(i+1), worker.GetDurable(), false, false, false, amqp.Table{"x-dead-letter-exchange": worker.GetQueue() + ".retry", "x-message-ttl": step})
		if err != nil {
			log.Println("Queue ", worker.GetQueue()+".retry."+strconv.Itoa(i+1), " declare error: ", err)
			return
		}
	}
	go func() {
		channel, err := RabbitMqConnect.Channel()
		defer channel.Close()
		if err != nil {
			log.Println("Channel: ", err)
			return
		}
		msgs, err := channel.Consume(worker.GetQueue(), "sneaker-go", false, false, false, false, nil)
		if err != nil {
			log.Println("Consume error: ", err)
		}
		for d := range msgs {
			exception := Exception{}
			response := excute(&worker, &d.Body, &exception)
			if exception.Msg != "" || !response[0].IsNil() {
				d.Headers["err"] = exception.Msg
				if exception.Msg == "" {
					d.Headers["err"] = response[0].String()
				}
				count, steps := 0, len(worker.GetSteps())
				if d.Headers["tryCount"] != nil {
					count = int(d.Headers["tryCount"].(int32))
				}
				if count < steps {
					err = retry(RabbitMqConnect, worker.GetQueue()+".retry."+strconv.Itoa(count+1), &d)
					if err != nil {
						log.Println("retry error: ", err)
					}
				} else {
					logFailedMessageInFailedQueue(RabbitMqConnect, worker.GetQueue(), &d.Body, &d)
				}
			}
			d.Ack(true)
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

func retry(RabbitMqConnect *amqp.Connection, queueName string, d *amqp.Delivery) (err error) {
	count := 0
	if (*d).Headers["tryCount"] != nil {
		count = int(d.Headers["tryCount"].(int32))
	}
	channel, err := RabbitMqConnect.Channel()
	defer channel.Close()
	err = channel.Publish("", queueName, false, false,
		amqp.Publishing{
			Headers: amqp.Table{
				"err":      d.Headers["err"],
				"tryCount": count + 1,
			},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            d.Body,
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
		},
	)
	if err != nil {
		log.Println("Publish error: ", err)
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
	_, err = channel.QueueDeclare(queueName+".faild", true, false, false, false, amqp.Table{})
	if err != nil {
		log.Println("Queue ", queueName+".faild", " declare error: ", err)
		return
	}
	err = (*channel).Publish("", queueName+".failed", false, false,
		amqp.Publishing{
			Headers: amqp.Table{
				"tryCount": count,
				"err":      (*d).Headers["err"],
			},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            *message,
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
		},
	)
	if err != nil {
		log.Println("Publish error: ", err)
		return
	}
	return
}
