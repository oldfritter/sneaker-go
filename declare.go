package sneaker

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

var errRabbitMqConnectionUnavailable = errors.New("rabbitmq connection is unavailable")

type RabbitMqConnect struct {
	*amqp.Connection
}

// IsClosed 返回连接是否为空或已关闭；对 nil 接收者安全。
func (conn *RabbitMqConnect) IsClosed() bool {
	return conn == nil || conn.Connection == nil || conn.Connection.IsClosed()
}

// Close 安全关闭连接；nil 或未初始化时返回 nil。
func (conn *RabbitMqConnect) Close() error {
	if conn == nil || conn.Connection == nil {
		return nil
	}
	return conn.Connection.Close()
}

// Channel 打开一个新 Channel，并在连接未初始化时返回错误。
func (conn *RabbitMqConnect) Channel() (*amqp.Channel, error) {
	if conn == nil || conn.Connection == nil {
		return nil, errRabbitMqConnectionUnavailable
	}
	channel, err := conn.Connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return channel, nil
}

// publish 在临时 Channel 上发布消息，并将 headers 作为消息属性发送。
func (conn *RabbitMqConnect) publish(exchange, key, contentType string, mandatory, immediate bool, message *[]byte, headers amqp.Table, deliveryMode uint8, expiration string) error {
	if message == nil {
		return errors.New("message body is nil")
	}
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	return channel.PublishWithContext(
		context.Background(),
		exchange,
		key,
		mandatory,
		immediate,
		amqp.Publishing{
			Headers:         headers,
			ContentType:     contentType,
			ContentEncoding: "",
			Body:            *message,
			DeliveryMode:    deliveryMode,
			Priority:        0,
			Expiration:      expiration,
		},
	)
}

// PublishMessageWithRouteKey 通过 Exchange 和 routing key 发布消息。
func (conn *RabbitMqConnect) PublishMessageWithRouteKey(exchange, routeKey, contentType string, mandatory, immediate bool, message *[]byte, arguments amqp.Table, deliveryMode uint8, expiration string) error {
	if err := conn.publish(exchange, routeKey, contentType, mandatory, immediate, message, arguments, deliveryMode, expiration); err != nil {
		return fmt.Errorf("publish to exchange %q with routing key %q: %w", exchange, routeKey, err)
	}
	return nil
}

// PublishMessageToQueue 通过默认 Exchange 直接发布到指定队列。
func (conn *RabbitMqConnect) PublishMessageToQueue(queue, contentType string, mandatory, immediate bool, message *[]byte, arguments amqp.Table, deliveryMode uint8, expiration string) error {
	if err := conn.publish("", queue, contentType, mandatory, immediate, message, arguments, deliveryMode, expiration); err != nil {
		return fmt.Errorf("publish to queue %q: %w", queue, err)
	}
	return nil
}

// DeclareQueue 在临时 Channel 上声明队列。
func (conn *RabbitMqConnect) DeclareQueue(queueName string, durable, autoDelete, internal, noWait bool, arguments amqp.Table) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if _, err = channel.QueueDeclare(queueName, durable, autoDelete, internal, noWait, arguments); err != nil {
		return fmt.Errorf("declare queue %q: %w", queueName, err)
	}
	return nil
}

// DeclareExchange 在临时 Channel 上声明 Exchange。
func (conn *RabbitMqConnect) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, arguments amqp.Table) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if err = channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, arguments); err != nil {
		return fmt.Errorf("declare exchange %q: %w", name, err)
	}
	return nil
}

// QueueBind 在临时 Channel 上绑定队列、routing key 和 Exchange。
func (conn *RabbitMqConnect) QueueBind(name, key, exchange string, noWait bool, arguments amqp.Table) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if err = channel.QueueBind(name, key, exchange, noWait, arguments); err != nil {
		return fmt.Errorf("bind queue %q to exchange %q with key %q: %w", name, exchange, key, err)
	}
	return nil
}
