package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/streadway/amqp"

	"github.com/oldfritter/sneaker-go/v3/example/config"
	"github.com/oldfritter/sneaker-go/v3/example/initializers"
)

var (
	closeChan = make(chan int)
)

func main() {
	initialize()
	initializers.InitWorkers()

	StartAllWorkers()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutdown Server ...")
	go recycle()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	select {
	case <-closeChan:
		cancel()
	case <-ctx.Done():
		cancel()
	}
}

func initialize() {
	initializers.InitializeAmqpConfig()

	setLog()
	err := os.MkdirAll("pids", 0755)
	if err != nil {
		log.Fatalf("create folder error: %v", err)
	}
	err = os.WriteFile("pids/workers.pid", []byte(strconv.Itoa(os.Getpid())), 0644)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}
}

func StartAllWorkers() {
	for _, w := range config.AllWorkerIs {
		for i := 0; i < w.GetThreads(); i++ {
			go func(w sneaker.WorkerI) {
				w.SetRabbitMqConnect(&initializers.RabbitMqConnect)
				w.InitLogger()
				sneaker.SubscribeMessageByQueue(w, amqp.Table{})
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

func recycle() {
	for i, worker := range config.AllWorkerIs {
		worker.Stop()
		worker.Recycle()
		log.Println("stoped: ", worker.GetName(), "[", i, "]")
	}
	closeChan <- 1
}
