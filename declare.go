package sneaker

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqConnect struct {
	*amqp.Connection
}

func (conn *RabbitMqConnect) PublishMessageWithRouteKey(exchange, routeKey, contentType string, mandatory, immediate bool, message *[]byte, arguments amqp.Table, deliveryMode uint8, expiration string) error {
	channel, err := conn.Channel()
	if err == nil {
		defer channel.Close()
	} else {
		log.Fatal(err)
		return fmt.Errorf("Channel: %s", err)
	}
	if err = channel.PublishWithContext(
		context.Background(),
		exchange,  // publish to an exchange
		routeKey,  // routing to 0 or more queues
		mandatory, // mandatory
		immediate, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     contentType,
			ContentEncoding: "",
			Body:            *message,
			DeliveryMode:    deliveryMode, // amqp.Persistent, amqp.Transient // 1=non-persistent, 2=transient
			Priority:        0,            // 0-9
			Expiration:      expiration,
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		log.Fatal(err)
		return fmt.Errorf("Exchange Publish: %s", err)
	}
	return nil
}

func (conn *RabbitMqConnect) PublishMessageToQueue(queue, contentType string, mandatory, immediate bool, message *[]byte, arguments amqp.Table, deliveryMode uint8, expiration string) error {
	channel, err := conn.Channel()
	if err == nil {
		defer channel.Close()
	} else {
		log.Fatal(err)
		return fmt.Errorf("Channel: %s", err)
	}
	if err = channel.PublishWithContext(
		context.Background(),
		"",        // publish to an exchange
		queue,     // routing to 0 or more queues
		mandatory, // mandatory
		immediate, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     contentType,
			ContentEncoding: "",
			Body:            *message,
			DeliveryMode:    deliveryMode, // amqp.Persistent, amqp.Transient // 1=non-persistent, 2=transient
			Priority:        0,            // 0-9
			Expiration:      expiration,
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		log.Fatal(err)
		return fmt.Errorf("Queue Publish: %s", err)
	}
	return nil
}

func (conn *RabbitMqConnect) DeclareQueue(queueName string, durable, autoDelete, internal, noWait bool, arguments amqp.Table) error {
	channel, err := conn.Channel()
	if err == nil {
		defer channel.Close()
	} else {
		log.Fatal(err)
		return fmt.Errorf("Channel: %s", err)
	}
	_, err = channel.QueueDeclare(queueName, durable, autoDelete, internal, noWait, arguments)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (conn *RabbitMqConnect) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, arguments amqp.Table) error {
	channel, err := conn.Channel()
	if err == nil {
		defer channel.Close()
	} else {
		log.Fatal(err)
		return fmt.Errorf("Channel: %s", err)
	}
	err = channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, arguments)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (conn *RabbitMqConnect) QueueBind(name, key, exchange string, noWait bool, arguments amqp.Table) error {
	channel, err := conn.Channel()
	if err == nil {
		defer channel.Close()
	} else {
		log.Fatal(err)
		return fmt.Errorf("Channel: %s", err)
	}
	err = channel.QueueBind(name, key, exchange, noWait, arguments)
	if err != nil {
		log.Fatal(err)
	}
	return err
}
