package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestConnectWithRetryRetriesUntilSuccess 验证拨号失败后会重试并最终成功。
func TestConnectWithRetryRetriesUntilSuccess(t *testing.T) {
	original := log.Writer()
	t.Cleanup(func() { log.SetOutput(original) })
	log.SetOutput(io.Discard)

	attempts := 0
	err := connectWithRetry(context.Background(), time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("broker offline")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("connectWithRetry: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

// TestConnectWithRetryStopsWhenContextIsCanceled 验证上下文取消会终止重试。
func TestConnectWithRetryStopsWhenContextIsCanceled(t *testing.T) {
	original := log.Writer()
	t.Cleanup(func() { log.SetOutput(original) })
	log.SetOutput(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	err := connectWithRetry(ctx, time.Hour, func() error {
		cancel()
		return errors.New("broker offline")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("connectWithRetry error = %v, want context.Canceled", err)
	}
}

// TestSetLogWritesToNestedPath 验证日志目录会自动创建并写入指定文件。
func TestSetLogWritesToNestedPath(t *testing.T) {
	original := log.Writer()
	t.Cleanup(func() { log.SetOutput(original) })

	path := filepath.Join(t.TempDir(), "nested", "workers.log")
	if err := setLog(path); err != nil {
		t.Fatalf("setLog: %v", err)
	}
	if file, ok := log.Writer().(*os.File); ok {
		t.Cleanup(func() { file.Close() })
	}
	log.Print("example log line")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(content), "example log line") {
		t.Fatalf("log = %q, want example log line", content)
	}
}

// TestWritePID 验证 PID 文件内容与当前进程号一致。
func TestWritePID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pids", "workers.pid")
	if err := writePID(path); err != nil {
		t.Fatalf("writePID: %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid: %v", err)
	}
	if got, want := string(content), strconv.Itoa(os.Getpid()); got != want {
		t.Fatalf("pid = %q, want %q", got, want)
	}
}

// TestInitializeReturnsMissingConfigError 验证缺少配置目录时会返回错误。
func TestInitializeReturnsMissingConfigError(t *testing.T) {
	if err := initialize(context.Background(), t.TempDir()); err == nil {
		t.Fatal("initialize accepted a missing config directory")
	}
}

// TestMonitorConnectionsStopsOnCanceledContext 验证监控循环会响应上下文取消。
func TestMonitorConnectionsStopsOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		monitorConnections(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitorConnections did not stop after context cancellation")
	}
}
