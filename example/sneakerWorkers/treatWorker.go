package sneakerWorkers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/oldfritter/sneaker-go/v3/example/config"
)

const TreatWorkerName = "TreatWorker"

// TreatWorkers 从配置列表中选出示例支持的 worker 实现。
func TreatWorkers(workers []sneaker.Worker) []sneaker.WorkerI {
	registered := make([]sneaker.WorkerI, 0, len(workers))
	for _, worker := range workers {
		worker.Name = strings.TrimSpace(worker.Name)
		if worker.Name == TreatWorkerName {
			registered = append(registered, &TreatWorker{Worker: worker})
		}
	}
	return registered
}

// InitializeTreatWorker 将示例 worker 注册到全局列表，供旧示例代码使用。
func InitializeTreatWorker() {
	config.AllWorkerIs = TreatWorkers(config.AllWorkers)
}

type TreatWorker struct {
	sneaker.Worker
}

// TreatPayload is the example message body.
type TreatPayload struct {
	ID int `json:"id"`
}

// Work 解析 JSON 消息并记录处理结果。
func (worker *TreatWorker) Work(payloadJSON *[]byte) error {
	start := time.Now()
	if payloadJSON == nil {
		worker.LogError("payload is nil")
		return errors.New("payload is nil")
	}

	var payload TreatPayload
	if err := json.Unmarshal(*payloadJSON, &payload); err != nil {
		worker.LogError("invalid payload: ", err)
		return fmt.Errorf("decode payload: %w", err)
	}

	worker.LogInfo("payload: ", payload, ", time:", time.Since(start))
	return nil
}
