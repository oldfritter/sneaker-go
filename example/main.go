package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"github.com/oldfritter/sneaker-go/v3/example/config"
	"github.com/oldfritter/sneaker-go/v3/example/initializers"
	amqp "github.com/rabbitmq/amqp091-go"
)

// main 解析启动参数，初始化配置和连接，并在退出信号到来时关闭服务。
func main() {
	configDir := flag.String("config", "config", "directory containing amqp.yml and workers.yml")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := initialize(ctx, *configDir); err != nil {
		log.Fatal(err)
	}
	prepareWorkers()
	StartAllWorkers()
	go monitorConnections(ctx)

	<-ctx.Done()
	log.Println("Shutdown Server ...")
	recycle()
}

// initialize 加载配置、初始化日志和 PID 文件，并等待 RabbitMQ 连接成功。
func initialize(ctx context.Context, configDir string) error {
	if err := initializers.InitializeAmqpConfig(filepath.Join(configDir, "amqp.yml")); err != nil {
		return err
	}
	if err := initializers.InitWorkers(filepath.Join(configDir, "workers.yml")); err != nil {
		return err
	}
	if err := setLog(filepath.Join("logs", "workers.log")); err != nil {
		return err
	}
	if err := writePID(filepath.Join("pids", "workers.pid")); err != nil {
		return err
	}
	return connectWithRetry(ctx, 5*time.Second, initializers.InitializeAmqpConnection)
}

// connectWithRetry 按固定间隔重试拨号，直到成功或上下文被取消。
func connectWithRetry(ctx context.Context, interval time.Duration, dial func() error) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		err := dial()
		if err == nil {
			return nil
		}
		log.Printf("connect rabbitmq: %v (retrying in %s)", err, interval)

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// monitorConnections 定期检查连接和 Channel，并在异常时恢复消费者。
func monitorConnections(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if initializers.IsAmqpConnectionClosed() {
			if err := initializers.InitializeAmqpConnection(); err != nil {
				log.Printf("reconnect rabbitmq: %v", err)
				continue
			}
			log.Println("RabbitMQ reconnected")
			StartAllWorkers()
			continue
		}
		for _, worker := range config.AllWorkerIs {
			if worker.IsChannelClosed() {
				subscribeWorker(worker)
			}
		}
	}
}

// prepareWorkers 在启动消费者前注入连接并初始化日志。
func prepareWorkers() {
	for _, worker := range config.AllWorkerIs {
		worker.SetRabbitMqConnect(initializers.RabbitMqConnect)
		worker.InitLogger()
	}
}

// StartAllWorkers 为每个已注册 worker 启动配置数量的消费者。
func StartAllWorkers() {
	for _, worker := range config.AllWorkerIs {
		worker.SetRabbitMqConnect(initializers.RabbitMqConnect)
		subscribeWorker(worker)
	}
}

// subscribeWorker 按 threads 配置启动一个或多个订阅。
func subscribeWorker(worker sneaker.WorkerI) {
	threads := worker.GetThreads()
	if threads < 1 {
		threads = 1
	}
	for index := 0; index < threads; index++ {
		go func() {
			if err := sneaker.SubscribeMessageByQueue(worker, amqp.Table{}); err != nil {
				log.Printf("subscribe %s: %v", worker.GetName(), err)
			}
		}()
	}
}

// setLog 创建日志目录并把标准日志输出重定向到指定文件。
func setLog(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create log folder for %q: %w", path, err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %q: %w", path, err)
	}
	log.SetOutput(file)
	return nil
}

// writePID 把当前进程号写入指定 PID 文件。
func writePID(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create pid folder for %q: %w", path, err)
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("write pid file %q: %w", path, err)
	}
	return nil
}

// recycle 停止 worker、关闭共享连接，并清空各 worker 的 Channel。
func recycle() {
	for _, worker := range config.AllWorkerIs {
		worker.Stop()
	}
	initializers.CloseAmqpConnection()
	for index, worker := range config.AllWorkerIs {
		worker.SetChannel(nil)
		log.Println("stopped: ", worker.GetName(), "[", index, "]")
	}
}
