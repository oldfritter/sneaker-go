package initializers

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/oldfritter/sneaker-go/v3/example/config"
	"github.com/oldfritter/sneaker-go/v3/example/sneakerWorkers"
	amqp "github.com/rabbitmq/amqp091-go"
)

// writeTestFile 在临时目录创建测试文件并返回路径。
func writeTestFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// TestInitializeAmqpConfig 验证 AMQP 配置文件会被加载到全局配置。
func TestInitializeAmqpConfig(t *testing.T) {
	original := AmqpGlobalConfig
	t.Cleanup(func() { AmqpGlobalConfig = original })

	path := writeTestFile(t, "amqp.yml", `
connect:
  host: rabbit
  port: "5672"
  username: guest
  password: guest
  vhost: /
`)
	if err := InitializeAmqpConfig(path); err != nil {
		t.Fatalf("InitializeAmqpConfig: %v", err)
	}
	if AmqpGlobalConfig.Connect.Host != "rabbit" {
		t.Fatalf("host = %q, want rabbit", AmqpGlobalConfig.Connect.Host)
	}
}

// TestInitWorkersRegistersTreatWorker 验证 TreatWorker 会被注册到全局列表。
func TestInitWorkersRegistersTreatWorker(t *testing.T) {
	originalWorkers, originalWorkerIs := config.AllWorkers, config.AllWorkerIs
	t.Cleanup(func() {
		config.AllWorkers = originalWorkers
		config.AllWorkerIs = originalWorkerIs
	})

	path := writeTestFile(t, "workers.yml", `
- name: TreatWorker
  queue: sneaker.test
`)
	if err := InitWorkers(path); err != nil {
		t.Fatalf("InitWorkers: %v", err)
	}
	if len(config.AllWorkers) != 1 || len(config.AllWorkerIs) != 1 {
		t.Fatalf("workers = %d, registered = %d, want 1 and 1", len(config.AllWorkers), len(config.AllWorkerIs))
	}
	if _, ok := config.AllWorkerIs[0].(*sneakerWorkers.TreatWorker); !ok {
		t.Fatalf("registered worker has type %T", config.AllWorkerIs[0])
	}
}

// TestInitWorkersRejectsUnsupportedWorker 验证没有受支持实现时返回错误。
func TestInitWorkersRejectsUnsupportedWorker(t *testing.T) {
	originalWorkers, originalWorkerIs := config.AllWorkers, config.AllWorkerIs
	t.Cleanup(func() {
		config.AllWorkers = originalWorkers
		config.AllWorkerIs = originalWorkerIs
	})

	path := writeTestFile(t, "workers.yml", `
- name: OtherWorker
  queue: sneaker.other
`)
	if err := InitWorkers(path); err == nil || !strings.Contains(err.Error(), "no supported worker") {
		t.Fatalf("InitWorkers error = %v, want unsupported-worker error", err)
	}
}

// TestInitializeAmqpConnectionUsesConfigURL 验证拨号使用配置生成并转义的 URI。
func TestInitializeAmqpConnectionUsesConfigURL(t *testing.T) {
	originalConfig, originalDial, originalConnection := AmqpGlobalConfig, dialAMQP, RabbitMqConnect
	t.Cleanup(func() {
		AmqpGlobalConfig = originalConfig
		dialAMQP = originalDial
		RabbitMqConnect = originalConnection
	})

	AmqpGlobalConfig = config.Amqp{Connect: config.Connect{
		Host:     "rabbit",
		Port:     "5672",
		Username: "guest",
		Password: "guest",
		Vhost:    "/team",
	}}
	var gotURI string
	dialAMQP = func(uri string) (*amqp.Connection, error) {
		gotURI = uri
		return nil, errors.New("broker offline")
	}

	err := InitializeAmqpConnection()
	if err == nil || !strings.Contains(err.Error(), "broker offline") {
		t.Fatalf("InitializeAmqpConnection error = %v", err)
	}
	want := "amqp://guest:guest@rabbit:5672/%2Fteam"
	if gotURI != want {
		t.Fatalf("dial URI = %q, want %q", gotURI, want)
	}
}

// TestConnectionHelpersAreNilSafe 验证连接辅助方法对 nil 连接安全。
func TestConnectionHelpersAreNilSafe(t *testing.T) {
	original := RabbitMqConnect
	t.Cleanup(func() { RabbitMqConnect = original })

	RabbitMqConnect = nil
	if !IsAmqpConnectionClosed() {
		t.Fatal("nil connection must report closed")
	}
	CloseAmqpConnection()
	if GetRabbitMqConnect() != nil {
		t.Fatal("CloseAmqpConnection left a connection behind")
	}

	RabbitMqConnect = &sneaker.RabbitMqConnect{}
	if !IsAmqpConnectionClosed() {
		t.Fatal("a connection without an underlying AMQP connection must report closed")
	}
	CloseAmqpConnection()
}
