package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sneaker "github.com/oldfritter/sneaker-go/v3"
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

// TestLoadAmqp 验证 amqp.yml 加载和 vhost URI 转义。
func TestLoadAmqp(t *testing.T) {
	path := writeTestFile(t, "amqp.yml", `
connect:
  host: rabbit
  port: "5672"
  username: guest
  password: guest
  vhost: /sneaker-go
`)
	cfg, err := LoadAmqp(path)
	if err != nil {
		t.Fatalf("LoadAmqp: %v", err)
	}
	if cfg.Connect.Host != "rabbit" || cfg.Connect.Vhost != "/sneaker-go" {
		t.Fatalf("unexpected config: %+v", cfg.Connect)
	}
	uri, err := cfg.URL()
	if err != nil {
		t.Fatalf("URL: %v", err)
	}
	want := "amqp://guest:guest@rabbit:5672/%2Fsneaker-go"
	if uri != want {
		t.Fatalf("URL = %q, want %q", uri, want)
	}
}

// TestAmqpURL 验证 AMQP URI 的默认值、IPv6 和特殊字符处理。
func TestAmqpURL(t *testing.T) {
	tests := []struct {
		name    string
		connect Connect
		want    string
		wantErr bool
	}{
		{
			name:    "default vhost",
			connect: Connect{Host: "rabbit", Port: "5672", Username: "guest", Password: "guest"},
			want:    "amqp://guest:guest@rabbit:5672/",
		},
		{
			name:    "default port and ipv6 host",
			connect: Connect{Host: "::1", Username: "guest", Password: "guest", Vhost: "/"},
			want:    "amqp://guest:guest@[::1]:5672/",
		},
		{
			name:    "escaped credentials and vhost",
			connect: Connect{Host: "rabbit", Port: "5672", Username: "user@name", Password: "p:ss", Vhost: "team/vhost"},
			want:    "amqp://user%40name:p%3Ass@rabbit:5672/team%2Fvhost",
		},
		{
			name:    "missing host",
			connect: Connect{Port: "5672"},
			wantErr: true,
		},
		{
			name:    "invalid port",
			connect: Connect{Host: "rabbit", Port: "not-a-port"},
			wantErr: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := (Amqp{Connect: testCase.connect}).URL()
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("URL = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("URL: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("URL = %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestLoadAmqpRejectsInvalidYAML 验证非法 YAML 会返回错误。
func TestLoadAmqpRejectsInvalidYAML(t *testing.T) {
	path := writeTestFile(t, "amqp.yml", "connect: [")
	if _, err := LoadAmqp(path); err == nil {
		t.Fatal("LoadAmqp accepted invalid YAML")
	}
}

// TestShippedExampleConfigsLoad 验证仓库自带示例配置可以被加载。
func TestShippedExampleConfigsLoad(t *testing.T) {
	if _, err := LoadAmqp("amqp.yml"); err != nil {
		t.Fatalf("LoadAmqp(amqp.yml): %v", err)
	}
	if _, err := LoadWorkers("workers.yml"); err != nil {
		t.Fatalf("LoadWorkers(workers.yml): %v", err)
	}
}

// TestLoadWorkers 验证 workers.yml 的字段和默认配置解析。
func TestLoadWorkers(t *testing.T) {
	path := writeTestFile(t, "workers.yml", `
- name: TreatWorker
  exchange: sneaker.example.default
  routing_key: sneaker.example.treat
  queue: sneaker.example.treat
  durable: true
  threads: 3
  steps:
    - 5000
    - 30000
  options:
    prefetch: "1"
`)
	workers, err := LoadWorkers(path)
	if err != nil {
		t.Fatalf("LoadWorkers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(workers))
	}
	worker := workers[0]
	if worker.Name != "TreatWorker" || worker.Queue != "sneaker.example.treat" {
		t.Fatalf("unexpected worker: %+v", worker)
	}
	if worker.GetThreads() != 3 {
		t.Fatalf("threads = %d, want 3", worker.GetThreads())
	}
	if worker.PrefetchCount() != 1 {
		t.Fatalf("prefetch = %d, want 1", worker.PrefetchCount())
	}
	if worker.MaxRetry() != 2 {
		t.Fatalf("max retry = %d, want 2", worker.MaxRetry())
	}
}

// TestValidateWorkers 验证 worker 配置的必填字段和组合约束。
func TestValidateWorkers(t *testing.T) {
	valid := sneaker.Worker{Name: "worker", Queue: "queue"}
	tests := []struct {
		name    string
		workers []sneaker.Worker
		want    string
	}{
		{name: "empty list", want: "at least one"},
		{name: "missing name", workers: []sneaker.Worker{{Queue: "queue"}}, want: "name is required"},
		{name: "missing queue", workers: []sneaker.Worker{{Name: "worker"}}, want: "queue is required"},
		{
			name:    "exchange without routing key",
			workers: []sneaker.Worker{{Name: "worker", Queue: "queue", Exchange: "exchange"}},
			want:    "exchange and routing_key",
		},
		{
			name:    "negative threads",
			workers: []sneaker.Worker{{Name: "worker", Queue: "queue", Threads: -1}},
			want:    "threads must not be negative",
		},
		{
			name:    "duplicate names",
			workers: []sneaker.Worker{valid, valid},
			want:    "duplicated",
		},
		{name: "valid queue-only worker", workers: []sneaker.Worker{valid}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := ValidateWorkers(testCase.workers)
			if testCase.want == "" {
				if err != nil {
					t.Fatalf("ValidateWorkers: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("ValidateWorkers error = %v, want containing %q", err, testCase.want)
			}
		})
	}
}
