package sneaker

import (
	"fmt"
	"log"
	"os"
)

const (
	DefaultLog = "logs/workers.log"
)

// log 按级别写入一条日志，未初始化 Logger 时回退到标准日志。
func (worker *Worker) log(level string, text ...interface{}) {
	logger := worker.Logger
	if logger == nil {
		logger = log.Default()
	}
	// SetPrefix mutates shared logger state. Formatting the level into each
	// record keeps concurrent worker goroutines from racing on that state.
	logger.Printf("%s: %s %s", level, worker.GetName(), fmt.Sprint(text...))
}

// LogInfo 写入 INFO 级别日志。
func (worker *Worker) LogInfo(text ...interface{}) {
	worker.log("INFO", text...)
}

// LogDebug 写入 DEBUG 级别日志。
func (worker *Worker) LogDebug(text ...interface{}) {
	worker.log("DEBUG", text...)
}

// LogError 写入 ERROR 级别日志。
func (worker *Worker) LogError(text ...interface{}) {
	worker.log("ERROR", text...)
}

// InitLogger 初始化 worker 日志；失败时回退到标准日志而不退出进程。
func (worker *Worker) InitLogger() {
	if err := worker.initLogger(); err != nil {
		log.Printf("worker %s: initialize logger: %v", worker.GetName(), err)
		if worker.Logger == nil {
			worker.Logger = log.Default()
		}
	}
}

// initLogger 创建日志目录和文件，并返回可处理的错误。
func (worker *Worker) initLogger() error {
	folder := worker.GetLogFolder()
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return fmt.Errorf("create log folder %q: %w", folder, err)
	}
	file, err := os.OpenFile(worker.GetLog(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %q: %w", worker.GetLog(), err)
	}
	worker.Logger = log.New(file, "", log.LstdFlags)
	return nil
}
