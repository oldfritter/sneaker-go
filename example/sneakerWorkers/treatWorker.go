package sneakerWorkers

import (
	"encoding/json"
	"time"
)

// 此处方法名需要同worker的名称一致
func (worker Worker) TreatWorker(payloadJson *[]byte) (err error) {
	start := time.Now().UnixNano()
	var payload struct {
		Id int `json:"id"`
	}
	err = json.Unmarshal([]byte(*payloadJson), &payload)
	if err != nil {
		worker.LogError("payload: ", payload, ", time:", (time.Now().UnixNano()-start)/1000000, " ms")
		return
	}
	panic("test panic")

	// 这里完成此worker的功能

	worker.LogInfo("payload: ", payload, ", time:", (time.Now().UnixNano()-start)/1000000, " ms")
	return
}
