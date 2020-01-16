package sneakerWorkers

import (
	"encoding/json"
	"fmt"
	"time"
)

func (worker Worker) TreatWorker(payloadJson *[]byte) (queueName string, message []byte) {
	start := time.Now().UnixNano()
	var payload struct {
		Id int `json:"id"`
	}
	json.Unmarshal([]byte(*payloadJson), &payload)
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "INFO--TreatWorker payload: ", payload, ", time:", (time.Now().UnixNano()-start)/1000000, " ms")
	return
}
