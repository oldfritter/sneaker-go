package initializers

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/oldfritter/sneaker-go/v3"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v2"
)

type Amqp struct {
	Connect struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Vhost    string `yaml:"vhost"`
	} `yaml:"connect"`

	Exchange map[string]map[string]string `yaml:"exchange"`
}

var (
	AmqpGlobalConfig Amqp
	RabbitMqConnect  sneaker.RabbitMqConnect
)

func InitializeAmqpConfig() {
	path_str, _ := filepath.Abs("config/amqp.yml")
	content, err := os.ReadFile(path_str)
	if err != nil {
		log.Fatal(err)
		return
	}
	err = yaml.Unmarshal(content, &AmqpGlobalConfig)
	if err != nil {
		log.Fatal(err)
		return
	}
	InitializeAmqpConnection()
}

func InitializeAmqpConnection() {
	var err error
	conn, err := amqp.Dial("amqp://" + AmqpGlobalConfig.Connect.Username + ":" + AmqpGlobalConfig.Connect.Password + "@" + AmqpGlobalConfig.Connect.Host + ":" + AmqpGlobalConfig.Connect.Port + "/" + AmqpGlobalConfig.Connect.Vhost)
	RabbitMqConnect = sneaker.RabbitMqConnect{Connection: conn}
	if err != nil {
		time.Sleep(5000)
		InitializeAmqpConnection()
		return
	}
	go func() {
		<-RabbitMqConnect.NotifyClose(make(chan *amqp.Error))
		InitializeAmqpConnection()
	}()
}

func IsAmqpConnectionClosed() bool {
	return RabbitMqConnect.IsClosed()
}

func CloseAmqpConnection() {
	RabbitMqConnect.Close()
}

func GetRabbitMqConnect() sneaker.RabbitMqConnect {
	return RabbitMqConnect
}
