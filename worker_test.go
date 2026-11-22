package sneaker

import (
	"bytes"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkerAccessors 验证 Worker 的字段读取、队列命名和默认值。
func TestWorkerAccessors(t *testing.T) {
	worker := &Worker{
		Name:         "worker",
		Exchange:     "exchange",
		ExchangeType: "direct",
		RoutingKey:   "key",
		Queue:        "queue",
		Log:          "logs/worker.log",
		Durable:      true,
		Options:      map[string]string{"prefetch": "2"},
		Arguments:    map[string]string{"x-max-priority": "5"},
		Steps:        []string{"1000", "5000"},
		Threads:      3,
	}

	if got := worker.GetName(); got != "worker" {
		t.Fatalf("GetName = %q, want worker", got)
	}
	if got := worker.GetExchange(); got != "exchange" {
		t.Fatalf("GetExchange = %q, want exchange", got)
	}
	if got := worker.GetExchangeType(); got != "direct" {
		t.Fatalf("GetExchangeType = %q, want direct", got)
	}
	if got := worker.GetRoutingKey(); got != "key" {
		t.Fatalf("GetRoutingKey = %q, want key", got)
	}
	if got := worker.GetQueue(); got != "queue" {
		t.Fatalf("GetQueue = %q, want queue", got)
	}
	if got := worker.GetDelayQueue(); got != "queue.delay" {
		t.Fatalf("GetDelayQueue = %q, want queue.delay", got)
	}
	if got := worker.GetRetryQueue(); got != "queue.retry" {
		t.Fatalf("GetRetryQueue = %q, want queue.retry", got)
	}
	if got := worker.GetFailedQueue(); got != "queue.failed" {
		t.Fatalf("GetFailedQueue = %q, want queue.failed", got)
	}
	if got := worker.GetLog(); got != "logs/worker.log" {
		t.Fatalf("GetLog = %q, want logs/worker.log", got)
	}
	if got := worker.GetLogFolder(); got != filepath.Dir("logs/worker.log") {
		t.Fatalf("GetLogFolder = %q, want %q", got, filepath.Dir("logs/worker.log"))
	}
	if !worker.GetDurable() {
		t.Fatal("GetDurable = false, want true")
	}
	if got := worker.GetOptions()["prefetch"]; got != "2" {
		t.Fatalf("GetOptions prefetch = %q, want 2", got)
	}
	if got := worker.GetArguments()["x-max-priority"]; got != "5" {
		t.Fatalf("GetArguments = %q, want 5", got)
	}
	if len(worker.GetSteps()) != 2 || worker.GetThreads() != 3 {
		t.Fatalf("steps/threads = %v/%d, want 2/3", worker.GetSteps(), worker.GetThreads())
	}
	if err := worker.Work(nil); err != nil {
		t.Fatalf("default Work = %v, want nil", err)
	}

	emptyWorker := &Worker{}
	if got := emptyWorker.GetExchangeType(); got != "topic" {
		t.Fatalf("default GetExchangeType = %q, want topic", got)
	}
	if got := emptyWorker.GetLog(); got != DefaultLog {
		t.Fatalf("default GetLog = %q, want %q", got, DefaultLog)
	}
}

// TestWorkerLifecycle 验证 worker 的就绪状态、连接注入和 Channel 清理。
func TestWorkerLifecycle(t *testing.T) {
	var logs bytes.Buffer
	worker := &Worker{Name: "worker", Logger: log.New(&logs, "", 0)}

	if worker.IsReady() {
		t.Fatal("new worker is ready")
	}
	worker.Start()
	if !worker.IsReady() {
		t.Fatal("Start did not mark the worker ready")
	}
	worker.Stop()
	if worker.IsReady() {
		t.Fatal("Stop did not mark the worker not ready")
	}

	if !worker.IsChannelClosed() {
		t.Fatal("new worker should have no open channel")
	}
	if channel := worker.GetChannel(); channel != nil {
		t.Fatalf("GetChannel without connection = %v, want nil", channel)
	}

	worker.SetRabbitMqConnect(&RabbitMqConnect{})
	if worker.GetRabbitMqConnect() == nil {
		t.Fatal("SetRabbitMqConnect did not store the connection")
	}
	if channel := worker.GetChannel(); channel != nil {
		t.Fatalf("GetChannel with uninitialized connection = %v, want nil", channel)
	}
	if !strings.Contains(logs.String(), "open channel") {
		t.Fatalf("log = %q, want open channel error", logs.String())
	}

	worker.SetChannel(nil)
	if !worker.IsChannelClosed() {
		t.Fatal("SetChannel(nil) did not clear the channel")
	}
	worker.Recycle()
	if worker.GetChannel() != nil {
		t.Fatal("Recycle left a channel behind")
	}
}

// TestWorkerPerformLogsErrors 验证 Perform 会记录序列化和发布错误。
func TestWorkerPerformLogsErrors(t *testing.T) {
	var logs bytes.Buffer
	worker := &Worker{Name: "worker", Exchange: "exchange", RoutingKey: "key", Logger: log.New(&logs, "", 0)}

	worker.Perform(make(chan int))
	if !strings.Contains(logs.String(), "marshal published message") {
		t.Fatalf("log = %q, want marshal error", logs.String())
	}

	logs.Reset()
	worker.Perform(map[string]int{"id": 1})
	if !strings.Contains(logs.String(), "rabbitmq connection is unavailable") {
		t.Fatalf("log = %q, want unavailable connection", logs.String())
	}

	logs.Reset()
	worker.SetRabbitMqConnect(&RabbitMqConnect{})
	worker.Perform(map[string]int{"id": 1})
	if !strings.Contains(logs.String(), "publish message") {
		t.Fatalf("log = %q, want publish error", logs.String())
	}
}

// TestWorkerLoggerLevels 验证三种日志级别都会写入指定 logger。
func TestWorkerLoggerLevels(t *testing.T) {
	var logs bytes.Buffer
	worker := &Worker{Name: "worker", Logger: log.New(&logs, "", 0)}

	worker.LogInfo("info")
	worker.LogDebug("debug")
	worker.LogError("error")

	for _, want := range []string{"INFO: worker", "DEBUG: worker", "ERROR: worker"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log = %q, want %q", logs.String(), want)
		}
	}
}

// TestInitLoggerFallsBackOnInvalidPath 验证日志初始化失败时会回退而不 panic。
func TestInitLoggerFallsBackOnInvalidPath(t *testing.T) {
	original := log.Writer()
	t.Cleanup(func() { log.SetOutput(original) })
	log.SetOutput(io.Discard)

	worker := &Worker{Name: "worker", Log: t.TempDir()}
	worker.InitLogger()
	if worker.Logger == nil {
		t.Fatal("InitLogger did not install a fallback logger")
	}
}

// TestWorkerOptionFallbacks 验证非法配置值会回退到默认值。
func TestWorkerOptionFallbacks(t *testing.T) {
	worker := &Worker{Options: map[string]string{
		"ack":       "not-a-bool",
		"prefetch":  "-1",
		"retry_ttl": "not-a-duration",
		"max_retry": "not-an-int",
	}}
	if !worker.AckEnabled() {
		t.Fatal("invalid ack option should fall back to true")
	}
	if got := worker.PrefetchCount(); got != defaultPrefetch {
		t.Fatalf("PrefetchCount = %d, want %d", got, defaultPrefetch)
	}
	if got := worker.RetryTTL(); got != defaultRetryTTL {
		t.Fatalf("RetryTTL = %v, want %v", got, defaultRetryTTL)
	}
	if got := worker.MaxRetry(); got != defaultMaxRetry {
		t.Fatalf("MaxRetry = %d, want %d", got, defaultMaxRetry)
	}
}

// TestAtomicNextConsumerSequence 验证消费者序号单调递增。
func TestAtomicNextConsumerSequence(t *testing.T) {
	first := atomicNextConsumerSequence()
	second := atomicNextConsumerSequence()
	if second != first+1 {
		t.Fatalf("sequence = %d then %d, want consecutive values", first, second)
	}
}
