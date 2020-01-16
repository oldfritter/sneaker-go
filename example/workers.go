package main

import (
	// "fmt"
	// "io/ioutil"
	"os"
	"os/signal"
	// "strconv"

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

	// err := ioutil.WriteFile("pids/workers.pid", []byte(strconv.Itoa(os.Getpid())), 0644)
	// if err != nil {
	// 	fmt.Println(err)
	// }
}

func closeResource() {
	utils.CloseAmqpConnection()
}

func StartAllWorkers() {
	for _, w := range sneakerWorkers.AllWorkers {
		for i := 0; i < w.GetThreads(); i++ {
			go func(w sneakerWorkers.Worker) {
				sneaker.SubscribeMessageByQueue(w, amqp.Table{})
			}(w)
		}
	}
}
