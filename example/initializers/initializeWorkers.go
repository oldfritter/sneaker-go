package initializers

import (
	"log"
	"os"
	"path/filepath"

	"github.com/oldfritter/sneaker-go/v3/example/config"
	"github.com/oldfritter/sneaker-go/v3/example/sneakerWorkers"
	"gopkg.in/yaml.v2"
)

func InitWorkers() {
	pathStr, _ := filepath.Abs("config/workers.yml")
	content, err := os.ReadFile(pathStr)
	if err != nil {
		log.Fatal(err)
	}
	yaml.Unmarshal(content, &config.AllWorkers)
	sneakerWorkers.InitializeTreatWorker()
}
