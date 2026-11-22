package initializers

import (
	"fmt"

	"github.com/oldfritter/sneaker-go/v3"
	"github.com/oldfritter/sneaker-go/v3/example/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	AmqpGlobalConfig config.Amqp
	RabbitMqConnect  *sneaker.RabbitMqConnect
	dialAMQP         = amqp.Dial
)

// InitializeAmqpConfig 从指定路径加载 RabbitMQ 配置。
func InitializeAmqpConfig(path string) error {
	cfg, err := config.LoadAmqp(path)
	if err != nil {
		return err
	}
	AmqpGlobalConfig = cfg
	return nil
}

// InitializeAmqpConnection 建立并替换当前 RabbitMQ 连接。
func InitializeAmqpConnection() error {
	uri, err := AmqpGlobalConfig.URL()
	if err != nil {
		return err
	}
	conn, err := dialAMQP(uri)
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	RabbitMqConnect = &sneaker.RabbitMqConnect{Connection: conn}
	return nil
}

// IsAmqpConnectionClosed 返回当前连接是否为空或已关闭。
func IsAmqpConnectionClosed() bool {
	return RabbitMqConnect == nil || RabbitMqConnect.IsClosed()
}

// CloseAmqpConnection 关闭当前连接并清空全局引用。
func CloseAmqpConnection() {
	if RabbitMqConnect != nil {
		_ = RabbitMqConnect.Close()
		RabbitMqConnect = nil
	}
}

// GetRabbitMqConnect 返回当前 RabbitMQ 连接。
func GetRabbitMqConnect() *sneaker.RabbitMqConnect {
	return RabbitMqConnect
}
