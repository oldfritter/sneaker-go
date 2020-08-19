package sneaker

import (
	"log"
	"os"
	"regexp"
	"strings"
)

const (
	DefaultLog = "logs/workers.log"
)

type Exception struct {
	Msg string
}

type WorkerI interface {
	Work(*[]byte) error
	GetName() string
	GetExchange() string
	GetRetryExchange() string
	GetExchangeType() string
	GetRoutingKey() string
	GetQueue() string
	GetRetryQueue() string
	GetFailedQueue() string
	GetLog() string
	GetLogFolder() string
	GetDurable() bool
	GetDelay() bool
	GetOptions() map[string]string
	GetArguments() map[string]string
	GetSteps() []string
	GetThreads() int
	InitLogger()
}

type Worker struct {
	Name         string            `yaml:"name"`
	Exchange     string            `yaml:"exchange"`
	ExchangeType string            `yaml:"exchange_type"` // default: topic
	RoutingKey   string            `yaml:"routing_key"`
	Queue        string            `yaml:"queue"`
	Log          string            `yaml:"log"`
	Durable      bool              `yaml:"durable"`
	Delay        bool              `yaml:"delay"`
	Options      map[string]string `yaml:"options"`
	Arguments    map[string]string `yaml:"arguments"`
	Steps        []string          `yaml:"steps"`
	Threads      int               `yaml:"threads"`

	Logger *log.Logger
}

func (worker *Worker) GetName() string {
	return worker.Name
}
func (worker *Worker) GetExchange() string {
	return worker.Exchange
}
func (worker *Worker) GetRetryExchange() string {
	return worker.Queue + ".retry"
}
func (worker *Worker) GetExchangeType() string {
	if worker.ExchangeType == "" {
		worker.ExchangeType = "topic"
	}
	return worker.ExchangeType
}
func (worker *Worker) GetRoutingKey() string {
	return worker.RoutingKey
}
func (worker *Worker) GetQueue() string {
	return worker.Queue
}
func (worker *Worker) GetRetryQueue() string {
	return worker.Queue + ".retry"
}
func (worker *Worker) GetFailedQueue() string {
	return worker.Queue + ".failed"
}
func (worker *Worker) GetLog() string {
	if worker.Log != "" {
		return worker.Log
	}
	return DefaultLog
}
func (worker *Worker) GetLogFolder() string {
	re := regexp.MustCompile(`\/.*\.log$`)
	return strings.TrimSuffix(worker.GetLog(), re.FindString(worker.GetLog()))
}
func (worker *Worker) GetDurable() bool {
	return worker.Durable
}
func (worker *Worker) GetDelay() bool {
	return worker.Delay
}
func (worker *Worker) GetOptions() map[string]string {
	return worker.Options
}
func (worker *Worker) GetArguments() map[string]string {
	return worker.Arguments
}
func (worker *Worker) GetSteps() []string {
	return worker.Steps
}
func (worker *Worker) GetThreads() int {
	return worker.Threads
}

func (worker *Worker) LogInfo(text ...interface{}) {
	worker.Logger.SetPrefix("INFO: " + worker.GetName() + " ")
	worker.Logger.Println(text)
}

func (worker *Worker) LogDebug(text ...interface{}) {
	worker.Logger.SetPrefix("DEBUG: " + worker.GetName() + " ")
	worker.Logger.Println(text)
}

func (worker *Worker) LogError(text ...interface{}) {
	worker.Logger.SetPrefix("ERROR: " + worker.GetName() + " ")
	worker.Logger.Println(text)
}

func (worker *Worker) InitLogger() {
	err := os.Mkdir(worker.GetLogFolder(), 0755)
	if err != nil {
		if !os.IsExist(err) {
			log.Fatalf("create folder error: %v", err)
		}
	}
	file, err := os.OpenFile(worker.GetLog(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("open file error: %v", err)
	}
	worker.Logger = log.New(file, "", log.LstdFlags)
}

func (worker *Worker) Work(body *[]byte) (err error) {
	return err
}
