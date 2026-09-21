package sneaker

import (
	"context"
	"errors"
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
	commits    int
	rollbacks  int
	publishErr error
	commitErr  error
	// acksInTransaction records whether the ack was issued while the
	// replacement publish was still inside its transaction.
	acksInTransaction bool
	// onPublish, onTx and onSettled observe the transaction boundaries, so a test
	// can prove that two transactions never overlap on one channel.
	onPublish func()
	onTx      func()
	onSettled func()

	// transactionsInFlight and maxConcurrentTransactions measure how many
	// transactions were open at once, which is the scope a rollback discards.
	transactionsInFlight      int
	maxConcurrentTransactions int
}

func (f *fakeChannel) Ack(tag uint64, multiple bool) error {
	f.acked = append(f.acked, tag)
	f.multiples = append(f.multiples, multiple)
	if f.txOpen {
		f.acksInTransaction = true
	}
	return nil
}

func (f *fakeChannel) Nack(tag uint64, multiple, requeue bool) error {
	f.nacks = append(f.nacks, tag)
	f.multiples = append(f.multiples, multiple)
	f.requeued = append(f.requeued, requeue)
	return nil
}

// Reject completes amqp.Acknowledger, which a delivery needs.
func (f *fakeChannel) Reject(tag uint64, requeue bool) error {
	f.nacks = append(f.nacks, tag)
	f.requeued = append(f.requeued, requeue)
	return nil
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

func (f *fakeChannel) Tx() error {
	f.txOpen = true
	if f.onTx != nil {
		f.onTx()
	}
	return nil
}

func (f *fakeChannel) TxCommit() error {
	f.txOpen = false
	f.commits++
	if f.onSettled != nil {
		f.onSettled()
	}
	return f.commitErr
}

func (f *fakeChannel) TxRollback() error {
	f.txOpen = false
	f.rollbacks++
	if f.onSettled != nil {
		f.onSettled()
	}
	return nil
}

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
}

type queueDeclare struct {
	name string
	args amqp.Table
}

type queueBinding struct {
	queue string
	key   string
}

func (f *fakeQueues) Ack(tag uint64, multiple bool) error           { return nil }
func (f *fakeQueues) Nack(tag uint64, multiple, requeue bool) error { return nil }
func (f *fakeQueues) Tx() error                                     { return nil }
func (f *fakeQueues) TxCommit() error                               { return nil }
func (f *fakeQueues) TxRollback() error                             { return nil }

func (f *fakeQueues) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}

func (f *fakeQueues) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	f.declares = append(f.declares, queueDeclare{name: name, args: args})
	return amqp.Queue{Name: name}, nil
}

func (f *fakeQueues) QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error {
	f.bindings = append(f.bindings, queueBinding{queue: queue, key: key})
	return nil
}

func (f *fakeQueues) Reject(tag uint64, requeue bool) error { return nil }

func (f *fakeQueues) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return nil
}

func (f *fakeQueues) Qos(prefetchCount, prefetchSize int, global bool) error { return nil }

func (f *fakeQueues) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if f.deliveries != nil {
		return f.deliveries, nil
	}
	messages := make(chan amqp.Delivery)
	close(messages)
	return messages, nil
}

func (f *fakeQueues) declaredQueues() map[string]bool {
	names := map[string]bool{}
	for _, declare := range f.declares {
		names[declare.name] = true
	}
	return names
}

func (f *fakeQueues) declaresOf(name string) []amqp.Table {
	var found []amqp.Table
	for _, declare := range f.declares {
		if declare.name == name {
			found = append(found, declare.args)
		}
	}
	return found
}

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

func newTestWorker(options map[string]string) *testWorker {
	return &testWorker{Worker: Worker{
		Name:    "TestWorker",
		Queue:   "sneaker.test",
		Options: options,
	}}
}

func newDelivery(acknowledger amqp.Acknowledger) *amqp.Delivery {
	return &amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  7,
		Headers:      amqp.Table{},
		ContentType:  "application/json",
		Body:         []byte(`{"id":1}`),
	}
}

// A broker outage must not look like a lost message: when the retry publish
// cannot be committed, the original delivery is rolled back and requeued, and
// it is never acknowledged.
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

// The publish and the removal of the original are committed together, so a
// publish failure cannot leave a parked copy behind while the original is
// acknowledged away.
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
	if channel.multiples[0] {
		t.Fatal("the ack covered every outstanding delivery, want a single delivery")
	}
}

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

// A panic the worker did not recover must not let its message leave without a
// trace: the delivery is parked in the retry topology, and its removal is
// committed together with that copy instead of being acked on its own.
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

func TestProcessDoesNotAckWhenAckDisabled(t *testing.T) {
	worker := newTestWorker(map[string]string{"ack": "false"})
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 0 {
		t.Fatal("ack:false acknowledged a processed message")
	}
}

func TestProcessDoesNotRequeueWhenRequeueDisabled(t *testing.T) {
	worker := newTestWorker(map[string]string{"retry": "false", "requeue": "false"})
	worker.err = errors.New("work failed")
	channel := &fakeChannel{}

	process(worker, channel, newDelivery(channel))

	if len(channel.acked) != 0 || len(channel.nacks) != 0 {
		t.Fatalf("acks = %v nacks = %v, want the message left untouched", channel.acked, channel.nacks)
	}
}

// An ack is never cumulative: a cumulative ack would settle deliveries that
// other goroutines are still processing.
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

// An exhausted message must reach the failed queue before the original is
// removed, so the ack only happens once the copy is on the server.
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

// The configured step of an attempt is its cumulative wait, so a stage queue
// holds the DELTA to the next stage: the chain then reproduces waiting 5s after
// the first failure, 30s of total waiting after the second, and 60s after the
// third, without ever delaying a message twice.
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

// The worker queue must be declared exactly once with the dead letter arguments,
// because the broker rejects an inequivalent redeclare with a 406 and closes the
// channel, which would stop every worker that configures a step.
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

// Every stage is bound by exactly its own name, so a copy addressed to one stage
// is never swallowed by the worker queue or by another stage.
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

// A copy is addressed to the stage of its attempt, and each attempt waits the
// cumulative delay of its own step.
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

// A commit that cannot be answered must not be followed by a second settle: the
// broker already knows the outcome and may have applied it. The only settle here
// is the one inside that transaction.
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

// Two deliveries that park their copies on one channel must not interleave their
// transactions, or a rollback would discard the other delivery's copy while its
// original is acknowledged.
func TestTransactionsOnOneChannelAreSerialized(t *testing.T) {
	channel := &fakeChannel{}
	worker := newTestWorker(nil)
	worker.Steps = []string{"5000"}
	worker.err = errors.New("work failed")

	// The fake records the transaction boundaries, and a transaction is open
	// from Tx to the matching commit or rollback. The counters are guarded,
	// because the deliveries are processed concurrently.
	var counterLock sync.Mutex
	channel.onTx = func() {
		counterLock.Lock()
		defer counterLock.Unlock()
		channel.transactionsInFlight++
		if channel.transactionsInFlight > channel.maxConcurrentTransactions {
			channel.maxConcurrentTransactions = channel.transactionsInFlight
		}
	}
	channel.onSettled = func() {
		counterLock.Lock()
		defer counterLock.Unlock()
		channel.transactionsInFlight--
	}
	channel.onPublish = func() {
		counterLock.Lock()
		defer counterLock.Unlock()
		if channel.transactionsInFlight != 1 {
			t.Error("a publish happened outside a transaction")
		}
	}

	var group sync.WaitGroup
	for index := 0; index < 8; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			process(worker, channel, newDelivery(channel))
		}()
	}
	group.Wait()

	counterLock.Lock()
	defer counterLock.Unlock()
	if channel.maxConcurrentTransactions > 1 {
		t.Fatalf("%d transactions ran at once, want at most 1 per channel", channel.maxConcurrentTransactions)
	}
	if channel.transactionsInFlight != 0 {
		t.Fatalf("transactions in flight = %d, want 0", channel.transactionsInFlight)
	}
}

// A panic inside the library must not kill the process and leave the worker
// consuming nothing.
func TestConsumeLoopSurvivesALibraryPanic(t *testing.T) {
	worker := newTestWorker(nil)
	channel := &fakeQueues{}
	messages := make(chan amqp.Delivery, 1)
	channel.deliveries = messages
	// A worker whose custom exchange name makes the publish path reach a
	// closed connection: the panic is recovered instead of ending the loop.
	worker.Name = ""

	done := make(chan struct{})
	go func() {
		defer close(done)
		consumeMessages(worker, channel)
	}()

	messages <- *newDelivery(&fakeChannel{})
	close(messages)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the consume loop did not finish after its channel was closed")
	}
}

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
