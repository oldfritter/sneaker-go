package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/streadway/amqp"

	"github.com/oldfritter/sneaker-go/v3/example/config"
	"github.com/oldfritter/sneaker-go/v3/example/initializers"
)

func main() {
	initialize()
	initializers.InitWorkers()

	StartAllWorkers()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	closeResource()
}

func initialize() {
	initializers.InitializeAmqpConfig()

	setLog()
	err := os.MkdirAll("pids", 0755)
	if err != nil {
		log.Fatalf("create folder error: %v", err)
	}
	err = ioutil.WriteFile("pids/workers.pid", []byte(strconv.Itoa(os.Getpid())), 0644)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}
}

func closeResource() {
	initializers.CloseAmqpConnection()
}

func StartAllWorkers() {
	for _, w := range config.AllWorkerIs {
		for i := 0; i < w.GetThreads(); i++ {
			go func(w sneaker.WorkerI) {
				w.InitLogger()
				sneaker.SubscribeMessageByQueue(initializers.RabbitMqConnect.Connection, w, amqp.Table{})
			}(w)
		}
	}
}

func setLog() {
	err := os.Mkdir("logs", 0755)
	if err != nil {
		if !os.IsExist(err) {
			log.Fatalf("create folder error: %v", err)
		}
	}
	file, err := os.OpenFile("logs/workers.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}
	log.SetOutput(file)
}
