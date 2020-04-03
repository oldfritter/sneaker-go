package sneakerWorkers

import (
	"encoding/json"
	"time"
)

// 此处方法名需要同worker的名称一致
func (worker Worker) TreatWorker(payloadJson *[]byte) (queueName string, message []byte) {
	start := time.Now().UnixNano()
	var payload struct {
		Id int `json:"id"`
	}
	json.Unmarshal([]byte(*payloadJson), &payload)

	// 这里完成此worker的功能
	// 如果有任何异常，为queueName和message赋值，该任务将在队列queueName中等待，稍后再次处理
	// 默认的queueName为当前worker的queue+"-"+数字序号

	worker.LogInfo(time.Now().Format("2006-01-02 15:04:05"), "INFO--TreatWorker payload: ", payload, ", time:", (time.Now().UnixNano()-start)/1000000, " ms")
	return
}
