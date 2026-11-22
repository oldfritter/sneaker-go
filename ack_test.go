package sneaker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeChannel stands in for the broker channel, so every acknowledgement
// decision is observable without a broker.
type fakeChannel struct {
	acked      []uint64
	nacks      []uint64
	requeued   []bool
	multiples  []bool
	published  []amqp.Publishing
	publishTo  []string
	txOpen     bool
	txCalls    int
	commits    int
	rollbacks  int
	publishErr error
	commitErr  error
	// acksInTransaction records whether the ack was issued while the
	// replacement publish was still inside its transaction. The count separates
	// the transaction's own ack from an unrelated ack that joined it.
	acksInTransaction     bool
	acksDuringTransaction int
	// onPublish, onTx and onSettled observe the transaction boundaries, so a test
	// can prove that two transactions never overlap on one channel.
	onPublish func()
	onTx      func()
	onSettled func()

	// transactionsInFlight and maxConcurrentTransactions measure how many
	// transactions were open at once, which is the scope a rollback discards.
	transactionsInFlight      int
	maxConcurrentTransactions int
	closed                    bool
}

// IsClosed 返回 fake channel 的关闭状态。
func (f *fakeChannel) IsClosed() bool { return f.closed }

// Ack 记录单条确认及其 multiple 标志。
func (f *fakeChannel) Ack(tag uint64, multiple bool) error {
	f.acked = append(f.acked, tag)
	f.multiples = append(f.multiples, multiple)
	if f.txOpen {
		f.acksInTransaction = true
		f.acksDuringTransaction++
	}
	return nil
}

// Nack 记录拒绝、multiple 和 requeue 标志。
func (f *fakeChannel) Nack(tag uint64, multiple, requeue bool) error {
	f.nacks = append(f.nacks, tag)
	f.multiples = append(f.multiples, multiple)
	f.requeued = append(f.requeued, requeue)
	return nil
}

// Reject 实现 amqp.Acknowledger，记录拒绝和 requeue 标志。
func (f *fakeChannel) Reject(tag uint64, requeue bool) error {
	f.nacks = append(f.nacks, tag)
	f.requeued = append(f.requeued, requeue)
	return nil
}

// QueueDeclare 返回一个以队列名命名的空队列。
func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

// Tx 记录一次事务开启。
func (f *fakeChannel) Tx() error {
	f.txOpen = true
	f.txCalls++
	if f.onTx != nil {
		f.onTx()
	}
	return nil
}

// TxCommit 记录一次事务提交。
func (f *fakeChannel) TxCommit() error {
	f.txOpen = false
	f.commits++
	if f.onSettled != nil {
		f.onSettled()
	}
	return f.commitErr
}

// TxRollback 记录一次事务回滚。
func (f *fakeChannel) TxRollback() error {
	f.txOpen = false
	f.rollbacks++
	if f.onSettled != nil {
		f.onSettled()
	}
	return nil
}

// PublishWithContext 记录一次发布及其目标。
func (f *fakeChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if f.onPublish != nil {
		f.onPublish()
	}
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published = append(f.published, msg)
	f.publishTo = append(f.publishTo, exchange+"/"+key)
	return nil
}

var _ deliveryChannel = (*fakeChannel)(nil)

// fakeQueues records the topology a build declares, so the declaration and
// binding arguments are observable without a broker.
type fakeQueues struct {
	declares   []queueDeclare
	bindings   []queueBinding
	deliveries chan amqp.Delivery

	qosCalls    int
	qosPrefetch int
	qosErr      error

	consumeCalls int
	consumeQueue string
	consumeTag   string
	consumeErr   error

	queueDeclareCalls  int
	queueDeclareErrAt  int
	queueDeclareErr    error
	exchangeDeclareErr error
	queueBindErr       error
}

type queueDeclare struct {
	name string
	args amqp.Table
}

type queueBinding struct {
	queue string
	key   string
}

// Ack 忽略确认操作。
func (f *fakeQueues) Ack(tag uint64, multiple bool) error { return nil }

// Nack 忽略拒绝操作。
func (f *fakeQueues) Nack(tag uint64, multiple, requeue bool) error { return nil }

// IsClosed 始终返回 false。
func (f *fakeQueues) IsClosed() bool { return false }

// Tx 忽略事务开启。
func (f *fakeQueues) Tx() error { return nil }

// TxCommit 忽略事务提交。
func (f *fakeQueues) TxCommit() error { return nil }

// TxRollback 忽略事务回滚。
func (f *fakeQueues) TxRollback() error { return nil }

// PublishWithContext 忽略发布。
func (f *fakeQueues) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}

// QueueDeclare 记录队列声明参数，并支持按调用次数注入错误。
func (f *fakeQueues) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	f.queueDeclareCalls++
	if f.queueDeclareErr != nil {
		return amqp.Queue{}, f.queueDeclareErr
	}
	if f.queueDeclareErrAt > 0 && f.queueDeclareCalls == f.queueDeclareErrAt {
		return amqp.Queue{}, errors.New("queue declare failed")
	}
	f.declares = append(f.declares, queueDeclare{name: name, args: args})
	return amqp.Queue{Name: name}, nil
}

// QueueBind 记录队列绑定关系，并返回注入的错误。
func (f *fakeQueues) QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error {
	if f.queueBindErr != nil {
		return f.queueBindErr
	}
	f.bindings = append(f.bindings, queueBinding{queue: queue, key: key})
	return nil
}

// Reject 忽略拒绝操作。
func (f *fakeQueues) Reject(tag uint64, requeue bool) error { return nil }

// ExchangeDeclare 记录或返回 Exchange 声明错误。
func (f *fakeQueues) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return f.exchangeDeclareErr
}

// Qos 记录预取设置并返回注入的错误。
func (f *fakeQueues) Qos(prefetchCount, prefetchSize int, global bool) error {
	f.qosCalls++
	f.qosPrefetch = prefetchCount
	return f.qosErr
}

// Consume 记录消费者参数并返回预设投递 channel 或错误。
func (f *fakeQueues) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	f.consumeCalls++
	f.consumeQueue = queue
	f.consumeTag = consumer
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	if f.deliveries != nil {
		return f.deliveries, nil
	}
	messages := make(chan amqp.Delivery)
	close(messages)
	return messages, nil
}

// declaredQueues 返回已声明队列的名称集合。
func (f *fakeQueues) declaredQueues() map[string]bool {
	names := map[string]bool{}
	for _, declare := range f.declares {
		names[declare.name] = true
	}
	return names
}

// declaresOf 返回指定队列的全部声明参数。
func (f *fakeQueues) declaresOf(name string) []amqp.Table {
	var found []amqp.Table
	for _, declare := range f.declares {
		if declare.name == name {
			found = append(found, declare.args)
		}
	}
	return found
}

// ttlOf 返回指定队列声明的消息 TTL。
func (f *fakeQueues) ttlOf(name string) int32 {
	for _, declare := range f.declares {
		if declare.name != name {
			continue
		}
		ttl, ok := declare.args["x-message-ttl"].(int32)
		if !ok {
			return 0
		}
		return ttl
	}
	return 0
}

// bindingsOf 返回指定队列绑定的 routing key。
func (f *fakeQueues) bindingsOf(queue string) []string {
	var keys []string
	for _, binding := range f.bindings {
		if binding.queue == queue {
			keys = append(keys, binding.key)
		}
	}
	return keys
}

type testWorker struct {
	Worker
	err      error
	handled  int64
	panicked bool
	// handler, when set, replaces the default behaviour.
	handler func() error
}

// Work 记录调用次数，并按测试配置返回错误或触发 panic。
func (worker *testWorker) Work(body *[]byte) error {
	// A consumer may process deliveries concurrently, so the counter is atomic.
	atomic.AddInt64(&worker.handled, 1)
	if worker.panicked {
		panic("worker panic")
	}
	if worker.handler != nil {
		return worker.handler()
	}
	return worker.err
}

// newTestWorker 创建带默认队列和可选配置的测试 worker。
func newTestWorker(options map[string]string) *testWorker {
	return &testWorker{Worker: Worker{
		Name:    "TestWorker",
		Queue:   "sneaker.test",
		Options: options,
	}}
}

// newDelivery 创建带固定标签和 JSON 消息体的测试投递。
func newDelivery(acknowledger amqp.Acknowledger) *amqp.Delivery {
	return &amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  7,
		Headers:      amqp.Table{},
		ContentType:  "application/json",
		Body:         []byte(`{"id":1}`),
	}
}

// TestProcessPublishesBeforeAcking 验证发布失败时原消息会回滚并重新入队。
func TestProcessPublishesBeforeAcking(t *testing.T) {
	worker := newTestWorker(map[string]string{"retry": "false"})
	channel := &fakeChannel{}
	delivery := newDelivery(channel)
	worker.err = errors.New("work failed")

	process(worker, channel, delivery)

	if got := atomic.LoadInt64(&worker.handled); got != 1 {
		t.Fatalf("work calls = %d, want 1", got)
	}
	if len(channel.acked) != 0 {
		t.Fatalf("acks = %v, want none: a failed message must not be acked before it was parked", channel.acked)
	}
	if len(channel.nacks) != 1 {
		t.Fatalf("nacks = %v, want one: a message that was not parked must be requeued", channel.nacks)
	}
	if !channel.requeued[0] {
		t.Fatal("the message was nacked without requeue, so it would be dropped")
	}
	if channel.nacks[0] != delivery.DeliveryTag {
		t.Fatalf("nacked tag = %d, want %d", channel.nacks[0], delivery.DeliveryTag)
	}
}

// TestPublishThenAckRemovesTheOriginalOnlyAfterTheCopy 验证副本提交前不会确认原消息。
func TestPublishThenAckRemovesTheOriginalOnlyAfterTheCopy(t *testing.T) {
	failed := errors.New("publish refused")
	for _, testCase := range []struct {
		name        string
		publishErr  error
		wantFailure bool
		wantCommits int
		wantAcks    int
	}{
		{name: "publish failed", publishErr: failed, wantFailure: true, wantCommits: 0, wantAcks: 0},
		{name: "publish succeeded", publishErr: nil, wantCommits: 1, wantAcks: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			channel := &fakeChannel{publishErr: testCase.publishErr}
			err := publishThenAck(channel, func() error {
				return channel.PublishWithContext(context.Background(), "exchange", "key", false, false, amqp.Publishing{Body: []byte("copy")})
			}, 7)

			if got := err != nil; got != testCase.wantFailure {
				t.Fatalf("publishThenAck error = %v, want failure = %v", err, testCase.wantFailure)
			}
			if testCase.publishErr == nil && len(channel.published) != 1 {
				t.Fatalf("published = %d, want the replacement published once", len(channel.published))
			}
			if channel.commits != testCase.wantCommits {
				t.Fatalf("commits = %d, want %d", channel.commits, testCase.wantCommits)
			}
			if len(channel.acked) != testCase.wantAcks {
				t.Fatalf("acks = %v, want %d: the original is removed in the same transaction as its copy", channel.acked, testCase.wantAcks)
			}
			if testCase.publishErr == nil {
				if !channel.acksInTransaction {
					t.Fatal("the original was removed outside the transaction of its copy")
				}
			}
			if testCase.publishErr != nil {
				if channel.rollbacks != 1 {
					t.Fatalf("rollbacks = %d, want 1", channel.rollbacks)
				}
				if err != testCase.publishErr {
					t.Fatalf("error = %v, want %v", err, testCase.publishErr)
				}
			}
		})
	}
}

// TestProcessAcksProcessedMessage 验证成功消息会被确认且不进入事务。
func TestProcessAcksProcessedMessage(t *testing.T) {
	worker := newTestWorker(nil)
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 1 {
		t.Fatalf("acks = %v, want one", channel.acked)
	}
	if len(channel.nacks) != 0 {
		t.Fatalf("nacks = %v, want none", channel.nacks)
	}
	if channel.commits != 0 {
		t.Fatalf("commits = %d, want 0 outside transaction mode", channel.commits)
	}
	if channel.multiples[0] {
		t.Fatal("the ack covered every outstanding delivery, want a single delivery")
	}
}

// TestProcessAcksRecoveredPanic 验证被捕获的 panic 会作为失败消息进入失败队列并完成结算。
func TestProcessAcksRecoveredPanic(t *testing.T) {
	worker := newTestWorker(nil)
	worker.panicked = true
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 1 {
		t.Fatalf("acks = %v, want one: a recovered panic is a processed message", channel.acked)
	}
	if len(channel.nacks) != 0 {
		t.Fatalf("nacks = %v, want none", channel.nacks)
	}
}

// TestProcessNeverAcksAPanickedMessage 验证未恢复的 panic 不会被单独 ack，而是与失败副本一起提交。
func TestProcessNeverAcksAPanickedMessage(t *testing.T) {
	worker := newTestWorker(nil)
	worker.handler = func() error { panic("worker panic") }
	channel := &fakeChannel{}
	delivery := newDelivery(channel)
	delivery.Headers["tryCount"] = int32(2)

	process(worker, channel, delivery)

	if len(channel.published) != 1 {
		t.Fatalf("published = %d, want the retry copy parked once", len(channel.published))
	}
	if !channel.acksInTransaction {
		t.Fatal("the original was removed outside the transaction of its copy, so a crash could lose or duplicate it")
	}
	if channel.commits != 1 {
		t.Fatalf("commits = %d, want the parked copy committed with its removal", channel.commits)
	}
	if channel.rollbacks != 0 {
		t.Fatalf("rollbacks = %d, want none", channel.rollbacks)
	}
	if len(channel.nacks) != 0 {
		t.Fatalf("nacks = %v, want none: the delivery was parked", channel.nacks)
	}
}

// TestProcessDoesNotAckWhenAckDisabled 验证 ack=false 时成功消息保持未确认。
func TestProcessDoesNotAckWhenAckDisabled(t *testing.T) {
	worker := newTestWorker(map[string]string{"ack": "false"})
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 0 {
		t.Fatal("ack:false acknowledged a processed message")
	}
}

// TestProcessDoesNotRequeueWhenRequeueDisabled 验证 requeue=false 时消息保持未结算。
func TestProcessDoesNotRequeueWhenRequeueDisabled(t *testing.T) {
	worker := newTestWorker(map[string]string{"retry": "false", "requeue": "false"})
	worker.err = errors.New("work failed")
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 0 || len(channel.nacks) != 0 {
		t.Fatalf("acks = %v nacks = %v, want the message left untouched", channel.acked, channel.nacks)
	}
}

// TestProcessNeverAcksCumulatively 验证确认始终是单条而不是累计确认。
func TestProcessNeverAcksCumulatively(t *testing.T) {
	worker := newTestWorker(map[string]string{"ack_multiple": "true"})
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 1 {
		t.Fatalf("acks = %v, want one", channel.acked)
	}
	if channel.multiples[0] {
		t.Fatal("the ack was cumulative, so it would settle deliveries still being processed elsewhere")
	}
}

// TestProcessNeverAcksAnUnparkedMessage 验证消息未写入失败队列前不会被单独确认。
func TestProcessNeverAcksAnUnparkedMessage(t *testing.T) {
	worker := newTestWorker(map[string]string{"max_retry": "0", "retry": "false"})
	worker.err = errors.New("work failed")
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 0 {
		t.Fatal("the message was acked although it was neither processed nor parked")
	}
	if len(channel.nacks) != 1 || !channel.requeued[0] {
		t.Fatal("the message was not requeued, so it would be withheld by the server")
	}
}

// TestRetryCountToleratesForeignHeaderTypes 验证 tryCount 的多种类型和非法值处理。
func TestRetryCountToleratesForeignHeaderTypes(t *testing.T) {
	cases := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{"missing", amqp.Table{}, 0},
		{"int32", amqp.Table{"tryCount": int32(2)}, 2},
		{"int64", amqp.Table{"tryCount": int64(2)}, 2},
		{"int", amqp.Table{"tryCount": 2}, 2},
		{"string", amqp.Table{"tryCount": "2"}, 2},
		{"negative int32", amqp.Table{"tryCount": int32(-1)}, 0},
		{"negative string", amqp.Table{"tryCount": "-1"}, 0},
		{"garbage", amqp.Table{"tryCount": "many"}, 0},
		{"unsupported", amqp.Table{"tryCount": struct{}{}}, 0},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			delivery := &amqp.Delivery{Headers: testCase.headers}
			if got := retryCount(delivery); got != testCase.want {
				t.Fatalf("retryCount = %d, want %d", got, testCase.want)
			}
		})
	}
}

// TestRetryDelaysNormalizesSteps 验证重试步骤会规范化为累计等待时间。
func TestRetryDelaysNormalizesSteps(t *testing.T) {
	cases := []struct {
		name       string
		steps      []string
		wantDelays []time.Duration
	}{
		{"empty", nil, nil},
		{
			"milliseconds",
			[]string{"5000", "30000", "60000"},
			[]time.Duration{5 * time.Second, 30 * time.Second, time.Minute},
		},
		{
			"units",
			[]string{"5s", "500ms"},
			[]time.Duration{5 * time.Second, 500 * time.Millisecond},
		},
		{
			"blank and invalid steps",
			[]string{"", "5s", "not-a-duration"},
			[]time.Duration{5 * time.Second},
		},
		{
			"sub millisecond is raised to the protocol minimum",
			[]string{"0", "5000"},
			[]time.Duration{time.Millisecond, 5 * time.Second},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			worker := newTestWorker(nil)
			worker.Steps = testCase.steps
			delays := retryDelays(worker)
			if len(delays) != len(testCase.wantDelays) {
				t.Fatalf("delays = %v, want %v", delays, testCase.wantDelays)
			}
			for index := range delays {
				if delays[index] != testCase.wantDelays[index] {
					t.Fatalf("delays = %v, want %v", delays, testCase.wantDelays)
				}
			}
		})
	}
}

// TestRetryStageTTLsAreCumulativeDeltas 验证各阶段 TTL 是累计等待时间的差值。
func TestRetryStageTTLsAreCumulativeDeltas(t *testing.T) {
	channel := &fakeQueues{}
	worker := newTestWorker(nil)
	worker.Steps = []string{"5000", "30000", "60000"}

	if err := buildRetryTopology(worker, channel); err != nil {
		t.Fatalf("buildRetryTopology: %v", err)
	}

	declared := channel.declaredQueues()
	for _, want := range []string{
		"sneaker.test.retry",
		"sneaker.test.retry.1",
		"sneaker.test.retry.2",
		"sneaker.test.retry.3",
	} {
		if !declared[want] {
			t.Fatalf("queue %s was not declared, declared: %v", want, declared)
		}
	}
	// Stage N is entered after attempt N, so its wait is the delta from the
	// cumulative wait of attempt N to the one of attempt N+1.
	for name, want := range map[string]int32{
		"sneaker.test.retry.1": 25000,
		"sneaker.test.retry.2": 30000,
		"sneaker.test.retry.3": 60000,
	} {
		if got := channel.ttlOf(name); got != want {
			t.Fatalf("ttl of %s = %d, want %d", name, got, want)
		}
	}
}

// TestWorkerQueueIsDeclaredOnceWithDeadLetterRouting 验证 worker 队列只声明一次且保留死信参数。
func TestWorkerQueueIsDeclaredOnceWithDeadLetterRouting(t *testing.T) {
	channel := &fakeQueues{}
	worker := newTestWorker(nil)
	worker.Steps = []string{"5000", "30000"}

	if err := buildTopology(worker, channel, amqp.Table{"x-max-priority": int32(5)}); err != nil {
		t.Fatalf("buildTopology: %v", err)
	}

	declares := channel.declaresOf("sneaker.test")
	if len(declares) != 1 {
		t.Fatalf("worker queue declared %d times, want 1", len(declares))
	}
	if got := declares[0]["x-dead-letter-routing-key"]; got != "sneaker.test.retry" {
		t.Fatalf("dead letter routing key = %v, want the retry queue", got)
	}
	if got := declares[0]["x-max-priority"]; got != int32(5) {
		t.Fatalf("caller arguments were dropped: x-max-priority = %v", got)
	}
}

// TestEveryStageIsBoundByItsOwnName 验证每个阶段队列只绑定自己的名称。
func TestEveryStageIsBoundByItsOwnName(t *testing.T) {
	channel := &fakeQueues{}
	worker := newTestWorker(nil)
	worker.Steps = []string{"5000", "30000"}

	if err := buildRetryTopology(worker, channel); err != nil {
		t.Fatalf("buildRetryTopology: %v", err)
	}

	for queue, want := range map[string]string{
		"sneaker.test":         "sneaker.test",
		"sneaker.test.failed":  "sneaker.test.failed",
		"sneaker.test.retry":   "sneaker.test.retry",
		"sneaker.test.retry.1": "sneaker.test.retry.1",
		"sneaker.test.retry.2": "sneaker.test.retry.2",
	} {
		keys := channel.bindingsOf(queue)
		if len(keys) != 1 || keys[0] != want {
			t.Fatalf("bindings of %s = %v, want exactly [%s]", queue, keys, want)
		}
		if keys[0] == "#" {
			t.Fatalf("%s is bound with a wildcard, so it would swallow other stages", queue)
		}
	}
}

// TestRetryPublishesTheCopyToTheStageOfTheAttempt 验证重试副本会发布到对应尝试的阶段队列。
func TestRetryPublishesTheCopyToTheStageOfTheAttempt(t *testing.T) {
	worker := newTestWorker(nil)
	worker.Steps = []string{"5000", "30000", "60000"}

	for attempt, wantStage := range map[int]string{
		0: "sneaker.test.retry.1",
		1: "sneaker.test.retry.2",
		2: "sneaker.test.retry.3",
	} {
		channel := &fakeChannel{}
		delivery := newDelivery(channel)
		delivery.Headers["tryCount"] = int32(attempt)
		worker.err = errors.New("work failed")

		process(worker, channel, delivery)

		if len(channel.publishTo) != 1 {
			t.Fatalf("attempt %d published %v, want one copy", attempt, channel.publishTo)
		}
		if got := channel.publishTo[0]; got != "sneaker.test/"+wantStage {
			t.Fatalf("attempt %d published to %s, want %s", attempt, got, wantStage)
		}
	}
}

// TestIndeterminateCommitIsNotSettledAgain 验证提交结果未知时不会再次结算消息。
func TestIndeterminateCommitIsNotSettledAgain(t *testing.T) {
	channel := &fakeChannel{commitErr: errors.New("connection closed")}
	worker := newTestWorker(nil)
	worker.Steps = []string{"5000"}
	worker.err = errors.New("work failed")
	delivery := newDelivery(channel)

	process(worker, channel, delivery)

	if len(channel.acked) != 1 || !channel.acksInTransaction {
		t.Fatalf("acks = %v inTransaction = %v, want the single ack of the transaction", channel.acked, channel.acksInTransaction)
	}
	if len(channel.nacks) != 0 {
		t.Fatalf("nacks = %v, want none: the delivery was settled again although the commit outcome is unknown", channel.nacks)
	}
	if channel.rollbacks != 0 {
		t.Fatalf("rollbacks = %d, want none: the transaction was already sent to the broker", channel.rollbacks)
	}
}

// TestDeliveriesOnOneChannelAreSettledSerially 验证同一 Channel 上的投递按串行方式结算。
func TestDeliveriesOnOneChannelAreSettledSerially(t *testing.T) {
	channel := &fakeChannel{}
	var inFlight, maxInFlight int64
	channel.onPublish = func() {
		current := atomic.AddInt64(&inFlight, 1)
		for {
			maximum := atomic.LoadInt64(&maxInFlight)
			if current <= maximum || atomic.CompareAndSwapInt64(&maxInFlight, maximum, current) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
	}

	worker := newTestWorker(nil)
	worker.Steps = []string{"5000"}
	worker.err = errors.New("work failed")
	settlement := newSettlement(channel)

	var group sync.WaitGroup
	for index := 0; index < 8; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			processDelivery(worker, settlement, newDelivery(channel))
		}()
	}
	group.Wait()

	if got := atomic.LoadInt64(&maxInFlight); got != 1 {
		t.Fatalf("maximum concurrent publishes = %d, want 1 per channel", got)
	}
	if len(channel.published) != 8 {
		t.Fatalf("published = %d, want 8", len(channel.published))
	}
	if channel.txCalls != 1 {
		t.Fatalf("Tx calls = %d, want 1: transaction mode is selected once per channel", channel.txCalls)
	}
	if channel.commits != 8 {
		t.Fatalf("commits = %d, want one commit per delivery", channel.commits)
	}
}

// TestAckWaitsForAnotherDeliveryTransaction 验证成功确认不会混入其他消息的事务。
func TestAckWaitsForAnotherDeliveryTransaction(t *testing.T) {
	channel := &fakeChannel{}
	publishStarted := make(chan struct{})
	releasePublish := make(chan struct{})
	channel.onPublish = func() {
		close(publishStarted)
		<-releasePublish
	}

	worker := newTestWorker(nil)
	worker.Steps = []string{"5000"}
	worker.err = errors.New("work failed")
	settlement := newSettlement(channel)

	retryDone := make(chan struct{})
	go func() {
		processDelivery(worker, settlement, newDelivery(channel))
		close(retryDone)
	}()
	<-publishStarted

	ackDone := make(chan struct{})
	go func() {
		acknowledge(worker, settlement, newDelivery(channel))
		close(ackDone)
	}()

	select {
	case <-ackDone:
		t.Fatal("ack completed while another delivery's transaction was open")
	case <-time.After(150 * time.Millisecond):
	}
	if channel.acksDuringTransaction != 0 {
		t.Fatalf("acks issued during the transaction = %d, want 0 before the retry publish is committed", channel.acksDuringTransaction)
	}

	close(releasePublish)
	for name, done := range map[string]<-chan struct{}{"retry": retryDone, "ack": ackDone} {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not finish after the transaction was released", name)
		}
	}

	if channel.acksDuringTransaction != 1 {
		t.Fatalf("acks issued during the transaction = %d, want only the retry transaction's ack", channel.acksDuringTransaction)
	}
	if channel.txCalls != 1 {
		t.Fatalf("Tx calls = %d, want 1: the later ack must reuse transaction mode", channel.txCalls)
	}
	if channel.commits != 2 {
		t.Fatalf("commits = %d, want the retry transaction and the later ack transaction", channel.commits)
	}
	if len(channel.acked) != 2 {
		t.Fatalf("acks = %v, want the retry ack and the successful delivery ack", channel.acked)
	}
}

// TestTransactionModeSurvivesRollback 验证事务回滚后 Channel 仍保持事务模式。
func TestTransactionModeSurvivesRollback(t *testing.T) {
	channel := &fakeChannel{}
	settlement := newSettlement(channel)
	failedPublish := errors.New("publish refused")

	if err := settlement.publishThenAck(func() error { return failedPublish }, 1); !errors.Is(err, failedPublish) {
		t.Fatalf("publishThenAck error = %v, want %v", err, failedPublish)
	}
	if err := settlement.ack(2); err != nil {
		t.Fatalf("ack after rollback: %v", err)
	}

	if channel.txCalls != 1 {
		t.Fatalf("Tx calls = %d, want 1", channel.txCalls)
	}
	if channel.rollbacks != 1 {
		t.Fatalf("rollbacks = %d, want 1", channel.rollbacks)
	}
	if channel.commits != 1 {
		t.Fatalf("commits = %d, want the later ack committed", channel.commits)
	}
	if len(channel.acked) != 1 || channel.acked[0] != 2 {
		t.Fatalf("acks = %v, want [2]", channel.acked)
	}
}

// TestSettlementRegistrySharesAndCleansChannelState 验证同一 Channel 共享结算状态并正确清理。
func TestSettlementRegistrySharesAndCleansChannelState(t *testing.T) {
	settlementRegistry.Lock()
	baseline := len(settlementRegistry.entries)
	settlementRegistry.Unlock()

	channel := &fakeChannel{}
	first := acquireSettlement(channel)
	second := acquireSettlement(channel)
	if first != second {
		t.Fatal("two acquisitions of one channel returned different settlements")
	}
	releaseSettlement(channel, first)
	releaseSettlement(channel, second)
	settlementRegistry.Lock()
	count := len(settlementRegistry.entries)
	settlementRegistry.Unlock()
	if count != baseline {
		t.Fatalf("settlement entries retained = %d, want baseline %d", count, baseline)
	}

	transactionalChannel := &fakeChannel{}
	settlement := acquireSettlement(transactionalChannel)
	if err := settlement.publishThenAck(func() error { return nil }, 1); err != nil {
		t.Fatalf("select transaction mode: %v", err)
	}
	releaseSettlement(transactionalChannel, settlement)
	settlementRegistry.Lock()
	count = len(settlementRegistry.entries)
	settlementRegistry.Unlock()
	if count != baseline+1 {
		t.Fatalf("transactional settlement entries = %d, want %d until the channel closes", count, baseline+1)
	}

	transactionalChannel.closed = true
	replacement := acquireSettlement(transactionalChannel)
	if replacement == settlement {
		t.Fatal("a closed transactional channel reused its old settlement")
	}
	releaseSettlement(transactionalChannel, replacement)
	settlementRegistry.Lock()
	count = len(settlementRegistry.entries)
	settlementRegistry.Unlock()
	if count != baseline {
		t.Fatalf("settlement entries retained after closed-channel sweep = %d, want baseline %d", count, baseline)
	}
}

// TestProcessParksExhaustedMessageInTransaction 验证重试耗尽的消息会在事务中写入失败队列。
func TestProcessParksExhaustedMessageInTransaction(t *testing.T) {
	worker := newTestWorker(map[string]string{"max_retry": "0"})
	worker.err = errors.New("work failed")
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.published) != 1 {
		t.Fatalf("published = %d, want the failed-queue copy", len(channel.published))
	}
	if len(channel.publishTo) != 1 || channel.publishTo[0] != "/sneaker.test.failed" {
		t.Fatalf("publish target = %v, want the failed queue", channel.publishTo)
	}
	if channel.commits != 1 || channel.rollbacks != 0 {
		t.Fatalf("commits = %d rollbacks = %d, want one committed transaction", channel.commits, channel.rollbacks)
	}
	if len(channel.acked) != 1 || !channel.acksInTransaction {
		t.Fatalf("acks = %v inTransaction = %v, want the transaction ack", channel.acked, channel.acksInTransaction)
	}
	if len(channel.nacks) != 0 {
		t.Fatalf("nacks = %v, want none: the message was parked", channel.nacks)
	}
}

// TestProcessMessagesBoundsConcurrency 验证并发消费数量受 GetThreads 限制。
func TestProcessMessagesBoundsConcurrency(t *testing.T) {
	worker := newTestWorker(nil)
	worker.Threads = 2

	var active, maxActive, handled int64
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	started := make(chan struct{}, 4)
	worker.handler = func() error {
		current := atomic.AddInt64(&active, 1)
		for {
			maximum := atomic.LoadInt64(&maxActive)
			if current <= maximum || atomic.CompareAndSwapInt64(&maxActive, maximum, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		atomic.AddInt64(&active, -1)
		atomic.AddInt64(&handled, 1)
		return nil
	}

	channel := &fakeQueues{}
	messages := make(chan amqp.Delivery, 4)
	for index := 0; index < 4; index++ {
		messages <- *newDelivery(channel)
	}
	close(messages)

	done := make(chan struct{})
	go func() {
		processMessages(worker, newSettlement(channel), messages)
		close(done)
	}()

	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("fewer than two handlers started")
		}
	}
	select {
	case <-started:
		releaseAll()
		<-done
		t.Fatal("more than two handlers ran concurrently")
	case <-time.After(150 * time.Millisecond):
	}

	releaseAll()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processMessages did not wait for in-flight handlers")
	}
	if got := atomic.LoadInt64(&maxActive); got != 2 {
		t.Fatalf("maximum concurrent handlers = %d, want 2", got)
	}
	if got := atomic.LoadInt64(&handled); got != 4 {
		t.Fatalf("handled deliveries = %d, want 4", got)
	}
}

// TestConsumeMessagesDrainsClosedChannel 验证投递 channel 关闭后消费循环会退出。
func TestConsumeMessagesDrainsClosedChannel(t *testing.T) {
	worker := newTestWorker(nil)
	channel := &fakeQueues{}
	messages := make(chan amqp.Delivery, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		consumeMessages(worker, newSettlement(channel), messages)
	}()

	messages <- *newDelivery(&fakeChannel{})
	close(messages)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the consume loop did not finish after its channel was closed")
	}
}

// TestRetryOptions 验证重试、确认和 prefetch 配置项能被正确读取。
func TestRetryOptions(t *testing.T) {
	worker := newTestWorker(map[string]string{
		"ack":          "false",
		"retry":        "false",
		"requeue":      "false",
		"ack_multiple": "true",
		"prefetch":     "5",
		"retry_ttl":    "2m",
		"max_retry":    "9",
	})
	if worker.AckEnabled() {
		t.Fatal("ack:false was ignored")
	}
	if worker.RetryEnabled() {
		t.Fatal("retry:false was ignored")
	}
	if worker.RequeueEnabled() {
		t.Fatal("requeue:false was ignored")
	}
	if !worker.AckMultiple() {
		t.Fatal("ack_multiple:true was ignored")
	}
	if got := worker.PrefetchCount(); got != 5 {
		t.Fatalf("prefetch = %d, want 5", got)
	}
	if got := worker.RetryTTL(); got != 2*time.Minute {
		t.Fatalf("retry_ttl = %v, want 2m", got)
	}
	if got := worker.MaxRetry(); got != 9 {
		t.Fatalf("max_retry = %d, want 9", got)
	}
}

// TestRetryDefaultsFollowSteps 验证未配置选项时重试次数和 TTL 的默认行为。
func TestRetryDefaultsFollowSteps(t *testing.T) {
	worker := newTestWorker(nil)
	if !worker.AckEnabled() || !worker.RequeueEnabled() || !worker.RetryEnabled() {
		t.Fatal("the acknowledgement defaults must keep acknowledging and requeueing")
	}
	if worker.AckMultiple() {
		t.Fatal("ack_multiple must stay off: it is only safe with one consumer goroutine")
	}
	if got := worker.RetryTTL(); got != defaultRetryTTL {
		t.Fatalf("retry_ttl = %v, want %v", got, defaultRetryTTL)
	}
	if got := worker.MaxRetry(); got != defaultMaxRetry {
		t.Fatalf("max_retry = %d, want %d", got, defaultMaxRetry)
	}

	worker.Steps = []string{"5000", "30000"}
	if got := worker.MaxRetry(); got != 2 {
		t.Fatalf("max_retry = %d, want the 2 configured steps", got)
	}
}

// TestOptionOverridesPerWorker 验证 worker 专属配置优先于全局配置。
func TestOptionOverridesPerWorker(t *testing.T) {
	worker := newTestWorker(map[string]string{
		"prefetch":             "2",
		"TestWorker.prefetch":  "7",
		"OtherWorker.prefetch": "9",
	})
	if got := worker.PrefetchCount(); got != 7 {
		t.Fatalf("prefetch = %d, want the worker specific value 7", got)
	}
	// The option of another worker must not leak into this one.
	worker.Name = "ThirdWorker"
	if got := worker.PrefetchCount(); got != 2 {
		t.Fatalf("prefetch = %d, want the shared value 2", got)
	}
	worker.Name = "OtherWorker"
	if got := worker.PrefetchCount(); got != 9 {
		t.Fatalf("prefetch = %d, want the worker specific value 9", got)
	}
}

// TestParseDurationRejectsGarbage 验证时长解析会拒绝空值和非法格式。
func TestParseDurationRejectsGarbage(t *testing.T) {
	for _, value := range []string{"", " ", "soon", "5 second", "-"} {
		if _, ok := parseDuration(value); ok {
			t.Fatalf("parseDuration(%q) was accepted", value)
		}
	}
	if got, ok := parseDuration("1500"); !ok || got != 1500*time.Millisecond {
		t.Fatalf("parseDuration(\"1500\") = %v, %v, want 1.5s", got, ok)
	}
}

// TestGetLogFolder 验证不同日志路径的目录提取结果。
func TestGetLogFolder(t *testing.T) {
	for logPath, want := range map[string]string{
		"logs/workers.log":       "logs",
		"worker.log":             ".",
		"logs/worker":            "logs",
		"logs/nested/worker.log": "logs/nested",
	} {
		worker := newTestWorker(nil)
		worker.Log = logPath
		if got := worker.GetLogFolder(); got != want {
			t.Fatalf("GetLogFolder(%q) = %q, want %q", logPath, got, want)
		}
	}
}

// TestInitLoggerCreatesNestedFolder 验证日志目录会自动创建且能写入内容。
func TestInitLoggerCreatesNestedFolder(t *testing.T) {
	worker := newTestWorker(nil)
	worker.Log = filepath.Join(t.TempDir(), "nested", "worker.log")

	worker.InitLogger()

	if worker.Logger == nil {
		t.Fatal("InitLogger left Logger nil")
	}
	if file, ok := worker.Logger.Writer().(*os.File); ok {
		defer file.Close()
	}
	worker.LogInfo("logger smoke test")
	info, err := os.Stat(worker.Log)
	if err != nil {
		t.Fatalf("log file was not created: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("log file is empty after LogInfo")
	}
}

// TestOptionIntRejectsSignedAndDecimalValues 验证整数配置项拒绝符号、小数和多余数字。
func TestOptionIntRejectsSignedAndDecimalValues(t *testing.T) {
	for _, value := range []string{"", "-3", "+3", "3.5", "3s2", "many"} {
		worker := newTestWorker(map[string]string{"prefetch": value})
		if number, ok := worker.OptionInt("prefetch"); ok {
			t.Fatalf("OptionInt(%q) = %d, true; want rejected", value, number)
		}
	}
	worker := newTestWorker(map[string]string{"prefetch": "3s"})
	if number, ok := worker.OptionInt("prefetch"); !ok || number != 3 {
		t.Fatalf("OptionInt(\"3s\") = %d, %v; want 3, true", number, ok)
	}
}

// TestGetExchangeTypeDoesNotMutateTheWorker 验证读取默认 Exchange 类型不会修改原字段。
func TestGetExchangeTypeDoesNotMutateTheWorker(t *testing.T) {
	worker := newTestWorker(nil)
	if got := worker.GetExchangeType(); got != "topic" {
		t.Fatalf("GetExchangeType = %q, want topic", got)
	}
	if worker.ExchangeType != "" {
		t.Fatalf("GetExchangeType mutated the configured value to %q", worker.ExchangeType)
	}
}

// TestRabbitMqConnectWithoutConnectionIsNilSafe 验证未初始化连接上的方法不会 panic。
func TestRabbitMqConnectWithoutConnectionIsNilSafe(t *testing.T) {
	var connect *RabbitMqConnect
	if !connect.IsClosed() {
		t.Fatal("a nil connection must report closed")
	}
	if err := connect.Close(); err != nil {
		t.Fatalf("Close on a nil connection = %v, want nil", err)
	}
	channel, err := connect.Channel()
	if err == nil || channel != nil {
		t.Fatalf("Channel = %v, %v; want nil and an error", channel, err)
	}
	body := []byte(`{}`)
	if err := connect.PublishMessageToQueue("queue", "application/json", false, false, &body, nil, amqp.Persistent, ""); err == nil {
		t.Fatal("publishing without a connection returned nil")
	}
}
