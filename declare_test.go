package sneaker

import (
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestRabbitMqConnectNilReceiverMethodsReturnErrors 验证连接方法在 nil 接收者上返回错误。
func TestRabbitMqConnectNilReceiverMethodsReturnErrors(t *testing.T) {
	var conn *RabbitMqConnect
	body := []byte(`{"id":1}`)
	table := amqp.Table{}

	if !conn.IsClosed() {
		t.Fatal("nil connection should report closed")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close on nil connection = %v, want nil", err)
	}
	if channel, err := conn.Channel(); err == nil || channel != nil {
		t.Fatalf("Channel = %v, %v; want nil and an error", channel, err)
	}
	if err := conn.PublishMessageWithRouteKey("exchange", "key", "application/json", false, false, &body, table, amqp.Persistent, ""); err == nil {
		t.Fatal("PublishMessageWithRouteKey accepted a nil connection")
	}
	if err := conn.PublishMessageToQueue("queue", "application/json", false, false, &body, table, amqp.Persistent, ""); err == nil {
		t.Fatal("PublishMessageToQueue accepted a nil connection")
	}
	if err := conn.DeclareQueue("queue", true, false, false, false, table); err == nil {
		t.Fatal("DeclareQueue accepted a nil connection")
	}
	if err := conn.DeclareExchange("exchange", "topic", true, false, false, false, table); err == nil {
		t.Fatal("DeclareExchange accepted a nil connection")
	}
	if err := conn.QueueBind("queue", "key", "exchange", false, table); err == nil {
		t.Fatal("QueueBind accepted a nil connection")
	}
}

// TestRabbitMqConnectPublishRejectsNilBody 验证消息体为空时不会打开 Channel。
func TestRabbitMqConnectPublishRejectsNilBody(t *testing.T) {
	conn := &RabbitMqConnect{}
	table := amqp.Table{}

	cases := map[string]func() error{
		"exchange": func() error {
			return conn.PublishMessageWithRouteKey("exchange", "key", "application/json", false, false, nil, table, amqp.Persistent, "")
		},
		"queue": func() error {
			return conn.PublishMessageToQueue("queue", "application/json", false, false, nil, table, amqp.Persistent, "")
		},
	}
	for name, publish := range cases {
		t.Run(name, func(t *testing.T) {
			err := publish()
			if err == nil || !strings.Contains(err.Error(), "message body is nil") {
				t.Fatalf("publish error = %v, want nil-body error", err)
			}
		})
	}
}
