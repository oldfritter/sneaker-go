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
	GetRoutingKey() string
	GetQueue() string
	GetDurable() bool
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
		log.Println("Channel: ", err)
		return
	}
	_, err = channel.QueueDeclare(worker.GetQueue(), worker.GetDurable(), false, false, false, arguments)
	if err != nil {
		log.Println("Queue ", worker.GetQueue(), " declare error: ", err)
		return
	}
	if worker.GetExchange() != "" && worker.GetRoutingKey() != "" {
		err = channel.ExchangeDeclare(worker.GetExchange(), "topic", worker.GetDurable(), false, false, false, nil)
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
			log.Println("Queue ", worker.GetQueue()+".delay."+strconv.Itoa(i+1), " declare error : ", err)
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
		msgs, err := channel.Consume(
			worker.GetQueue(), // queue
			"sneaker-go",      // consumer
			false,             // auto-ack
			false,             // exclusive
			false,             // no-local
			false,             // no-wait
			nil,               // args
		)
		if err != nil {
			log.Println("Consume error: ", err)
		}
		for d := range msgs {
			exception := Exception{}
			response := excute(&worker, &d.Body, &exception)
			if exception.Msg != "" || !response[0].IsNil() {
				d.Headers = make(map[string]interface{})
				d.Headers["err"] = exception.Msg
				if exception.Msg == "" {
					d.Headers["err"] = response[0].String()
				}
				count := 0
				if d.Headers["tryCount"] != nil {
					count = int(d.Headers["tryCount"].(int32))
				}
				if count < len(worker.GetSteps()) {
					err = retry(RabbitMqConnect, worker.GetQueue(), &d.Body, &d)
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
		log.Println("Publish error: ", err)
		return
	}
	return
}
