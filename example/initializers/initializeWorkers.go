package initializers

import (
	"fmt"

	"github.com/oldfritter/sneaker-go/v3/example/config"
	"github.com/oldfritter/sneaker-go/v3/example/sneakerWorkers"
)

// InitWorkers 读取 workers.yml，并注册示例支持的全部 worker 实现。
func InitWorkers(path string) error {
	workers, err := config.LoadWorkers(path)
	if err != nil {
		return err
	}
	config.AllWorkers = workers
	config.AllWorkerIs = sneakerWorkers.TreatWorkers(workers)
	if len(config.AllWorkerIs) == 0 {
		return fmt.Errorf("%q contains no supported worker implementation", path)
	}
	return nil
}
