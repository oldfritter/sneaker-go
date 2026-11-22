package sneakerWorkers

import (
	"bytes"
	"log"
	"strings"
	"testing"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/oldfritter/sneaker-go/v3/example/config"
)

// newTestTreatWorker 创建带缓冲区日志的测试 worker。
func newTestTreatWorker(logs *bytes.Buffer) *TreatWorker {
	return &TreatWorker{Worker: sneaker.Worker{
		Name:   TreatWorkerName,
		Queue:  "sneaker.test",
		Logger: log.New(logs, "", 0),
	}}
}

// TestTreatWorkersSelectsOnlySupportedDefinitions 验证只注册 TreatWorker。
func TestTreatWorkersSelectsOnlySupportedDefinitions(t *testing.T) {
	workers := []sneaker.Worker{
		{Name: "OtherWorker", Queue: "other"},
		{Name: " TreatWorker ", Queue: "treat"},
	}
	registered := TreatWorkers(workers)
	if len(registered) != 1 {
		t.Fatalf("registered = %d, want 1", len(registered))
	}
	worker, ok := registered[0].(*TreatWorker)
	if !ok {
		t.Fatalf("registered type = %T, want *TreatWorker", registered[0])
	}
	if worker.GetName() != TreatWorkerName {
		t.Fatalf("worker name = %q, want %q", worker.GetName(), TreatWorkerName)
	}
}

// TestTreatWorkerWork 验证合法 JSON 消息会被成功处理。
func TestTreatWorkerWork(t *testing.T) {
	var logs bytes.Buffer
	worker := newTestTreatWorker(&logs)
	body := []byte(`{"id":42}`)

	if err := worker.Work(&body); err != nil {
		t.Fatalf("Work: %v", err)
	}
	if !strings.Contains(logs.String(), "payload") || !strings.Contains(logs.String(), "42") {
		t.Fatalf("log = %q, want payload and id", logs.String())
	}
}

// TestTreatWorkerWorkRejectsInvalidJSON 验证非法 JSON 会返回错误并记录日志。
func TestTreatWorkerWorkRejectsInvalidJSON(t *testing.T) {
	var logs bytes.Buffer
	worker := newTestTreatWorker(&logs)
	body := []byte(`{"id":`)

	if err := worker.Work(&body); err == nil {
		t.Fatal("Work accepted invalid JSON")
	}
	if !strings.Contains(logs.String(), "invalid payload") {
		t.Fatalf("log = %q, want invalid payload", logs.String())
	}
}

// TestTreatWorkerWorkRejectsNilBody 验证 nil 消息体不会导致 panic。
func TestTreatWorkerWorkRejectsNilBody(t *testing.T) {
	var logs bytes.Buffer
	worker := newTestTreatWorker(&logs)

	if err := worker.Work(nil); err == nil {
		t.Fatal("Work accepted a nil payload")
	}
	if !strings.Contains(logs.String(), "payload is nil") {
		t.Fatalf("log = %q, want nil-payload message", logs.String())
	}
}

// TestInitializeTreatWorkerUpdatesGlobalRegistry 验证兼容入口会更新全局注册表。
func TestInitializeTreatWorkerUpdatesGlobalRegistry(t *testing.T) {
	originalWorkers, originalWorkerIs := config.AllWorkers, config.AllWorkerIs
	t.Cleanup(func() {
		config.AllWorkers = originalWorkers
		config.AllWorkerIs = originalWorkerIs
	})

	config.AllWorkers = []sneaker.Worker{{Name: TreatWorkerName, Queue: "treat"}}
	InitializeTreatWorker()
	if len(config.AllWorkerIs) != 1 {
		t.Fatalf("registered = %d, want 1", len(config.AllWorkerIs))
	}
}
