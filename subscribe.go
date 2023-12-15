package sneaker

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeMessageByQueue(worker WorkerI, arguments amqp.Table) (err error) {
	channel := worker.GetChannel()
	if _, err = channel.QueueDeclare(
		worker.GetQueue(),
		worker.GetDurable(),
		false,
		false,
		false,
		arguments,
	); err != nil {
		log.Println("Queue ", worker.GetQueue(), " declare error: ", err)
		return
	}
	if worker.GetExchange() != "" && worker.GetRoutingKey() != "" {
		if err = channel.ExchangeDeclare(
			worker.GetExchange(),
			worker.GetExchangeType(),
			worker.GetDurable(),
			false,
			false,
			false,
			nil,
		); err != nil {
			log.Println("Exchange ", worker.GetExchange(), " declare error: ", err)
			return
		}
		if err = channel.QueueBind(
			worker.GetQueue(),
			worker.GetRoutingKey(),
			worker.GetExchange(),
			false,
			nil,
		); err != nil {
			log.Println("Queue ", worker.GetQueue(), " bind error: ", err)
			return
		}
	}
	if worker.GetDelay() {
		if _, err = channel.QueueDeclare(
			worker.GetDelayQueue(),
			worker.GetDurable(),
			false,
			false,
			false,
			amqp.Table{
				"x-dead-letter-exchange": worker.GetRetryExchange(),
			},
		); err != nil {
			log.Println("Queue ", worker.GetDelayQueue()+" declare error : ", err)
			return
		}
	}
	go func() {
		msgs, err := channel.Consume(
			worker.GetQueue(),
			worker.GetName(),
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			log.Println("Consume error: ", err)
		}
		for d := range msgs {
			exception := Exception{}
			err := excute(worker, &d.Body, &exception)
			if exception.Msg != "" || err != nil {
				d.Ack(true)
			}
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

func retry(worker WorkerI, d *amqp.Delivery) (err error) {
	channel, err := worker.GetRabbitMqConnect().Channel()
	if err == nil {
		defer channel.Close()
	}
	err = channel.PublishWithContext(
		context.Background(),
		"",
		worker.GetRetryQueue(),
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         d.Body,
			DeliveryMode: amqp.Persistent,
			Priority:     0,
		},
	)
	if err != nil {
		log.Println("Publish error: ", err)
		return
	}
	return
}

func logFailedMessageInFailedQueue(worker WorkerI, message *[]byte, d *amqp.Delivery) (err error) {
	channel, err := worker.GetRabbitMqConnect().Channel()
	if err == nil {
		defer channel.Close()
	}
	if _, err = channel.QueueDeclare(
		worker.GetFailedQueue(),
		true,
		false,
		false,
		false,
		amqp.Table{},
	); err != nil {
		log.Println("Queue ", worker.GetFailedQueue(), " declare error: ", err)
		return
	}
	err = (*channel).PublishWithContext(
		context.Background(),
		"",
		worker.GetFailedQueue(),
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         *message,
			DeliveryMode: amqp.Persistent,
			Priority:     0,
		},
	)
	if err != nil {
		log.Println("Publish error: ", err)
		return
	}
	return
}
