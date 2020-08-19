package sneaker

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func SubscribeMessageByQueue(RabbitMqConnect *amqp.Connection, worker WorkerI, arguments amqp.Table) (err error) {
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
			err = channel.ExchangeDeclare(worker.GetRetryExchange(), "topic", worker.GetDurable(), false, false, false, nil)
			if err != nil {
				log.Println("Exchange ", worker.GetRetryExchange(), " declare error: ", err)
				return
			}
			err = channel.QueueBind(worker.GetQueue(), "#", worker.GetRetryExchange(), false, nil)
			if err != nil {
				log.Println("Queue ", worker.GetQueue(), " bind error: ", err)
				return
			}
			_, err = channel.QueueDeclare(worker.GetRetryQueue(), worker.GetDurable(), false, false, false, amqp.Table{"x-dead-letter-exchange": worker.GetRetryExchange()})
			if err != nil {
				log.Println("Queue ", worker.GetRetryQueue()+" declare error: ", err)
				return
			}
		}
	}
	if worker.GetDelay() {
		_, err = channel.QueueDeclare(worker.GetQueue()+".delay", worker.GetDurable(), false, false, false, amqp.Table{"x-dead-letter-exchange": worker.GetRetryExchange()})
		if err != nil {
			log.Println("Queue ", worker.GetQueue()+".delay declare error : ", err)
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
			err := excute(worker, &d.Body, &exception)
			if exception.Msg != "" || err != nil {
				d.Headers["err"] = exception.Msg
				if exception.Msg == "" {
					d.Headers["err"] = err
				}
				count, steps := 0, len(worker.GetSteps())
				if d.Headers["tryCount"] != nil {
					count = int(d.Headers["tryCount"].(int32))
				}
				if count < steps {
					err = retry(RabbitMqConnect, worker, &d)
					if err != nil {
						log.Println("retry error: ", err)
					}
				} else {
					logFailedMessageInFailedQueue(RabbitMqConnect, worker, &d.Body, &d)
				}
			}
			d.Ack(true)
		}
	}()
	return
}

func excute(worker WorkerI, body *[]byte, exception *Exception) (err error) {
	defer func(e *Exception) {
		r := recover()
		if r != nil {
			e.Msg = fmt.Sprintf("%v", r)
		}
	}(exception)
	err = worker.Work(body)
	return
}

func retry(RabbitMqConnect *amqp.Connection, worker WorkerI, d *amqp.Delivery) (err error) {
	count := 0
	if (*d).Headers["tryCount"] != nil {
		count = int(d.Headers["tryCount"].(int32))
	}
	channel, err := RabbitMqConnect.Channel()
	defer channel.Close()
	err = channel.Publish("", worker.GetRetryQueue(), false, false,
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
			Expiration:      worker.GetSteps()[count+1],
		},
	)
	if err != nil {
		log.Println("Publish error: ", err)
		return
	}
	return
}

func logFailedMessageInFailedQueue(RabbitMqConnect *amqp.Connection, worker WorkerI, message *[]byte, d *amqp.Delivery) (err error) {
	count := 0
	if (*d).Headers["tryCount"] != nil {
		count = int((*d).Headers["tryCount"].(int32))
	}
	channel, err := RabbitMqConnect.Channel()
	defer channel.Close()
	_, err = channel.QueueDeclare(worker.GetFailedQueue(), true, false, false, false, amqp.Table{})
	if err != nil {
		log.Println("Queue ", worker.GetFailedQueue(), " declare error: ", err)
		return
	}
	err = (*channel).Publish("", worker.GetFailedQueue(), false, false,
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
