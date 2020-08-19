package sneakerWorkers

import (
	"encoding/json"
	"fmt"
	"time"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/oldfritter/sneaker-go/v3/example/config"
)

func InitializeTreatWorker() {
	for _, w := range config.AllWorkers {
		if w.Name == "TreatWorker" {
			config.AllWorkerIs = append(config.AllWorkerIs, &TreatWorker{w})
			return
		}
	}
}

type TreatWorker struct {
	sneaker.Worker
}

func (worker *TreatWorker) Work(payloadJson *[]byte) (err error) {
	fmt.Println("start payload: ", string(*payloadJson))
	start := time.Now().UnixNano()
	var payload struct {
		Id int `json:"id"`
	}
	err = json.Unmarshal(*payloadJson, &payload)
	if err != nil {
		worker.LogError("payload: ", payload, ", time:", (time.Now().UnixNano()-start)/1000000, " ms")
		return
	}
	fmt.Println("payload: ", payload)
	// panic("test panic")

	// 这里完成此worker的功能

	worker.LogInfo("payload: ", payload, ", time:", (time.Now().UnixNano()-start)/1000000, " ms")
	return
}
