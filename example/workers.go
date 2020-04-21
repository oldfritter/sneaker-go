package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"

	sneaker "github.com/oldfritter/sneaker-go"
	"github.com/oldfritter/sneaker-go/utils"
	"github.com/streadway/amqp"

	"github.com/oldfritter/sneaker-go/example/sneakerWorkers"
)

func main() {
	initialize()
	sneakerWorkers.InitWorkers()

	StartAllWorkers()

	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	closeResource()
}

func initialize() {
	utils.InitializeAmqpConfig()

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
	utils.CloseAmqpConnection()
}

func StartAllWorkers() {
	for _, w := range sneakerWorkers.AllWorkers {
		for i := 0; i < w.GetThreads(); i++ {
			go func(w sneakerWorkers.Worker) {
				sneaker.SubscribeMessageByQueue(utils.RabbitMqConnect, w, amqp.Table{})
			}(w)
		}
	}
}
