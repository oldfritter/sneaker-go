package sneaker

import (
	"log"
	"os"
)

const (
	DefaultLog = "logs/workers.log"
)

func (worker *Worker) LogInfo(text ...interface{}) {
	worker.Logger.SetPrefix("INFO: " + worker.GetName() + " ")
	worker.Logger.Println(text...)
}
func (worker *Worker) LogDebug(text ...interface{}) {
	worker.Logger.SetPrefix("DEBUG: " + worker.GetName() + " ")
	worker.Logger.Println(text...)
}
func (worker *Worker) LogError(text ...interface{}) {
	worker.Logger.SetPrefix("ERROR: " + worker.GetName() + " ")
	worker.Logger.Println(text...)
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
