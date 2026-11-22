package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	sneaker "github.com/oldfritter/sneaker-go/v3"
	"gopkg.in/yaml.v2"
)

const defaultAMQPPort = "5672"

// Connect is the broker section of amqp.yml.
type Connect struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Vhost    string `yaml:"vhost"`
}

// Amqp is the example broker configuration. Exchange is reserved for
// deployments that declare additional topology from configuration.
type Amqp struct {
	Connect  Connect                      `yaml:"connect"`
	Exchange map[string]map[string]string `yaml:"exchange"`
}

// readYAML 读取并反序列化 YAML 文件。
func readYAML(path string, target interface{}) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %q: %w", path, err)
	}
	if err := yaml.Unmarshal(content, target); err != nil {
		return fmt.Errorf("parse %q: %w", path, err)
	}
	return nil
}

// LoadAmqp 读取并校验 amqp.yml。
func LoadAmqp(path string) (Amqp, error) {
	var cfg Amqp
	if err := readYAML(path, &cfg); err != nil {
		return Amqp{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Amqp{}, fmt.Errorf("validate %q: %w", path, err)
	}
	return cfg, nil
}

// Validate 校验 AMQP 连接配置。
func (cfg Amqp) Validate() error {
	if strings.TrimSpace(cfg.Connect.Host) == "" {
		return fmt.Errorf("connect.host is required")
	}
	if port := strings.TrimSpace(cfg.Connect.Port); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return fmt.Errorf("connect.port %q is not a valid TCP port", cfg.Connect.Port)
		}
	}
	return nil
}

// URL 构造经过转义的 AMQP URI。
func (cfg Amqp) URL() (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	host := strings.TrimSpace(cfg.Connect.Host)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	port := strings.TrimSpace(cfg.Connect.Port)
	if port == "" {
		port = defaultAMQPPort
	}
	vhost := cfg.Connect.Vhost
	if vhost == "" {
		vhost = "/"
	}

	uri := &url.URL{
		Scheme: "amqp",
		Host:   net.JoinHostPort(host, port),
	}
	if cfg.Connect.Username != "" || cfg.Connect.Password != "" {
		uri.User = url.UserPassword(cfg.Connect.Username, cfg.Connect.Password)
	}
	if vhost == "/" {
		uri.Path = "/"
	} else {
		uri.Path = "/" + vhost
		uri.RawPath = "/" + url.PathEscape(vhost)
	}
	return uri.String(), nil
}

// LoadWorkers 读取并校验 workers.yml。
func LoadWorkers(path string) ([]sneaker.Worker, error) {
	var workers []sneaker.Worker
	if err := readYAML(path, &workers); err != nil {
		return nil, err
	}
	if err := ValidateWorkers(workers); err != nil {
		return nil, fmt.Errorf("validate %q: %w", path, err)
	}
	return workers, nil
}

// ValidateWorkers 校验 worker 列表的必填字段和组合约束。
func ValidateWorkers(workers []sneaker.Worker) error {
	if len(workers) == 0 {
		return fmt.Errorf("at least one worker is required")
	}
	names := make(map[string]bool, len(workers))
	for index, worker := range workers {
		label := fmt.Sprintf("worker %d", index+1)
		name := strings.TrimSpace(worker.Name)
		if name != "" {
			label = fmt.Sprintf("worker %q", name)
		}
		if name == "" {
			return fmt.Errorf("%s: name is required", label)
		}
		if names[name] {
			return fmt.Errorf("%s: name is duplicated", label)
		}
		names[name] = true
		if strings.TrimSpace(worker.Queue) == "" {
			return fmt.Errorf("%s: queue is required", label)
		}
		hasExchange := strings.TrimSpace(worker.Exchange) != ""
		hasRoutingKey := strings.TrimSpace(worker.RoutingKey) != ""
		if hasExchange != hasRoutingKey {
			return fmt.Errorf("%s: exchange and routing_key must be set together", label)
		}
		if worker.Threads < 0 {
			return fmt.Errorf("%s: threads must not be negative", label)
		}
	}
	return nil
}
