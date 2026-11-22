package sneaker

import (
	"errors"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestConsumerTag 验证空名称和自定义名称的消费者标签。
func TestConsumerTag(t *testing.T) {
	if got := consumerTag(""); got != "sneaker-go" {
		t.Fatalf("consumerTag(\"\") = %q, want sneaker-go", got)
	}
	if got := consumerTag("worker"); got != "worker" {
		t.Fatalf("consumerTag(worker) = %q, want worker", got)
	}
}

// TestMergeTable 验证参数表合并时 overrides 覆盖 base。
func TestMergeTable(t *testing.T) {
	merged := mergeTable(
		amqp.Table{"keep": 1, "override": "base"},
		map[string]interface{}{"override": "new", "add": 2},
	)
	if got := merged["keep"]; got != 1 {
		t.Fatalf("merged keep = %v, want 1", got)
	}
	if got := merged["override"]; got != "new" {
		t.Fatalf("merged override = %v, want new", got)
	}
	if got := merged["add"]; got != 2 {
		t.Fatalf("merged add = %v, want 2", got)
	}
}

// TestConsumeConfiguresQosAndTag 验证 consume 设置 QoS 并生成带前缀的消费者标签。
func TestConsumeConfiguresQosAndTag(t *testing.T) {
	channel := &fakeQueues{}
	worker := newTestWorker(map[string]string{"prefetch": "2"})

	msgs, err := consume(worker, channel)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if msgs == nil {
		t.Fatal("consume returned a nil delivery channel")
	}
	if channel.qosCalls != 1 || channel.qosPrefetch != 2 {
		t.Fatalf("qos calls/prefetch = %d/%d, want 1/2", channel.qosCalls, channel.qosPrefetch)
	}
	if channel.consumeCalls != 1 || channel.consumeQueue != worker.GetQueue() {
		t.Fatalf("consume calls/queue = %d/%q", channel.consumeCalls, channel.consumeQueue)
	}
	if !strings.HasPrefix(channel.consumeTag, "TestWorker-") {
		t.Fatalf("consumer tag = %q, want TestWorker-*", channel.consumeTag)
	}
}

// TestConsumeSkipsQosWhenPrefetchIsZero 验证 prefetch=0 时不调用 Qos。
func TestConsumeSkipsQosWhenPrefetchIsZero(t *testing.T) {
	channel := &fakeQueues{}
	worker := newTestWorker(nil)

	if _, err := consume(worker, channel); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if channel.qosCalls != 0 {
		t.Fatalf("qos calls = %d, want 0", channel.qosCalls)
	}
}

// TestConsumeReturnsQosError 验证 QoS 设置失败会向上返回错误。
func TestConsumeReturnsQosError(t *testing.T) {
	channel := &fakeQueues{qosErr: errors.New("qos refused")}
	worker := newTestWorker(map[string]string{"prefetch": "1"})

	_, err := consume(worker, channel)
	if err == nil || !strings.Contains(err.Error(), "channel qos") {
		t.Fatalf("consume error = %v, want channel qos error", err)
	}
}

// TestConsumeReturnsConsumeError 验证创建消费者失败会向上返回错误。
func TestConsumeReturnsConsumeError(t *testing.T) {
	channel := &fakeQueues{consumeErr: errors.New("consume refused")}
	worker := newTestWorker(nil)

	_, err := consume(worker, channel)
	if !errors.Is(err, channel.consumeErr) {
		t.Fatalf("consume error = %v, want %v", err, channel.consumeErr)
	}
}

// TestSubscribeFunctionsRejectMissingChannel 验证没有 Channel 时订阅立即失败。
func TestSubscribeFunctionsRejectMissingChannel(t *testing.T) {
	worker := newTestWorker(nil)
	for name, subscribe := range map[string]func(WorkerI, amqp.Table) error{
		"serial":     SubscribeMessageByQueue,
		"concurrent": SubscribeMessages,
	} {
		t.Run(name, func(t *testing.T) {
			if err := subscribe(worker, amqp.Table{}); err == nil || !strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("subscribe error = %v, want unavailable-channel error", err)
			}
		})
	}
}

// TestPrepareSubscriptionSuccess 验证订阅准备会声明拓扑并启动消费者。
func TestPrepareSubscriptionSuccess(t *testing.T) {
	channel := &fakeQueues{}
	worker := newTestWorker(nil)

	settlement, msgs, err := prepareSubscription(worker, channel, amqp.Table{})
	if err != nil {
		t.Fatalf("prepareSubscription: %v", err)
	}
	defer releaseSettlement(channel, settlement)
	if msgs == nil {
		t.Fatal("prepareSubscription returned a nil delivery channel")
	}
	if channel.consumeCalls != 1 {
		t.Fatalf("consume calls = %d, want 1", channel.consumeCalls)
	}
	if len(channel.declares) == 0 {
		t.Fatal("prepareSubscription did not declare topology")
	}
}

// TestPrepareSubscriptionReleasesSettlementOnTopologyError 验证拓扑失败会释放结算状态。
func TestPrepareSubscriptionReleasesSettlementOnTopologyError(t *testing.T) {
	channel := &fakeQueues{queueDeclareErr: errors.New("declare refused")}
	worker := newTestWorker(nil)

	settlementRegistry.Lock()
	baseline := len(settlementRegistry.entries)
	settlementRegistry.Unlock()

	if _, _, err := prepareSubscription(worker, channel, amqp.Table{}); err == nil {
		t.Fatal("prepareSubscription accepted a topology error")
	}
	settlementRegistry.Lock()
	count := len(settlementRegistry.entries)
	settlementRegistry.Unlock()
	if count != baseline {
		t.Fatalf("settlement entries = %d, want baseline %d", count, baseline)
	}
}

// TestBuildTopologyReportsErrors 验证拓扑声明各阶段的错误会向上返回。
func TestBuildTopologyReportsErrors(t *testing.T) {
	worker := newTestWorker(nil)
	tests := []struct {
		name    string
		channel *fakeQueues
		want    string
	}{
		{"queue declare", &fakeQueues{queueDeclareErr: errors.New("queue declare")}, "declare"},
		{"exchange declare", &fakeQueues{exchangeDeclareErr: errors.New("exchange declare")}, "exchange"},
		{"queue bind", &fakeQueues{queueBindErr: errors.New("queue bind")}, "bind"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			worker.Exchange = "exchange"
			worker.RoutingKey = "key"
			err := buildTopology(worker, testCase.channel, amqp.Table{})
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("buildTopology error = %v, want containing %q", err, testCase.want)
			}
		})
	}
}

// TestBuildRetryTopologyReportsErrors 验证重试拓扑声明错误会向上返回。
func TestBuildRetryTopologyReportsErrors(t *testing.T) {
	worker := newTestWorker(nil)
	worker.Steps = []string{"1000"}

	t.Run("exchange declare", func(t *testing.T) {
		channel := &fakeQueues{exchangeDeclareErr: errors.New("exchange declare")}
		if err := buildRetryTopology(worker, channel); err == nil {
			t.Fatal("buildRetryTopology accepted an exchange error")
		}
	})
	t.Run("queue bind", func(t *testing.T) {
		channel := &fakeQueues{queueBindErr: errors.New("queue bind")}
		if err := buildRetryTopology(worker, channel); err == nil {
			t.Fatal("buildRetryTopology accepted a bind error")
		}
	})
	t.Run("retry queue declare", func(t *testing.T) {
		channel := &fakeQueues{queueDeclareErrAt: 1}
		if err := buildRetryTopology(worker, channel); err == nil {
			t.Fatal("buildRetryTopology accepted a queue declare error")
		}
	})
}
