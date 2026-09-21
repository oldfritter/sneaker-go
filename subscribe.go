package sneaker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SubscribeMessageByQueue declares the worker topology and consumes the worker
// queue. Acknowledging happens in acknowledge, after the message was processed
// or handed to the retry topology: a message must never be acked before its
// outcome is known, otherwise a crash loses it.
func SubscribeMessageByQueue(worker WorkerI, arguments amqp.Table) (err error) {
	channel := worker.GetChannel()
	if channel == nil {
		return fmt.Errorf("channel for queue %s is unavailable", worker.GetQueue())
	}
	if err = buildTopology(worker, channel, arguments); err != nil {
		return
	}
	go consumeMessages(worker, channel)
	return
}

// SubscribeMessages declares the topology like SubscribeMessageByQueue and
// processes every delivery in its own goroutine. Deliveries are handed out by
// the delivery lock of the connection, so the concurrency of a worker is
// bounded by its consumer count, not by the goroutines started here.
func SubscribeMessages(worker WorkerI, arguments amqp.Table) (err error) {
	channel := worker.GetChannel()
	if channel == nil {
		return fmt.Errorf("channel for queue %s is unavailable", worker.GetQueue())
	}
	if err = buildTopology(worker, channel, arguments); err != nil {
		return
	}
	go func() {
		msgs, err := consume(worker, channel)
		if err != nil {
			log.Println("Consume error: ", err)
			return
		}
		for d := range msgs {
			go func(d amqp.Delivery) {
				process(worker, channel, &d)
			}(d)
		}
	}()
	return
}

// buildTopology declares the worker queue, its failed queue and the redelivery
// chain. A queue is declared exactly once, because redeclaring it with
// different arguments is rejected by the broker with a 406 and closes the
// channel.
func buildTopology(worker WorkerI, channel topologyChannel, arguments amqp.Table) (err error) {
	queue, durable := worker.GetQueue(), worker.GetDurable()
	// The dead letter arguments belong to the single declare: they are what
	// routes a rejected message into the retry chain instead of back to the
	// queue, which is what would happen without a dead letter key on a
	// queue-named exchange binding every routing key.
	if _, err = channel.QueueDeclare(queue, durable, false, false, false, mergeTable(arguments, map[string]interface{}{
		"x-dead-letter-exchange":    queue,
		"x-dead-letter-routing-key": worker.GetRetryQueue(),
	})); err != nil {
		return fmt.Errorf("queue %s declare: %w", queue, err)
	}
	if _, err = channel.QueueDeclare(worker.GetFailedQueue(), durable, false, false, false, nil); err != nil {
		return fmt.Errorf("queue %s declare: %w", worker.GetFailedQueue(), err)
	}
	if worker.GetExchange() == "" || worker.GetRoutingKey() == "" {
		return nil
	}
	if err = channel.ExchangeDeclare(
		worker.GetExchange(),
		worker.GetExchangeType(),
		durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("exchange %s declare: %w", worker.GetExchange(), err)
	}
	if err = channel.QueueBind(queue, worker.GetRoutingKey(), worker.GetExchange(), false, nil); err != nil {
		return fmt.Errorf("queue %s bind: %w", queue, err)
	}
	return buildRetryTopology(worker, channel)
}

func mergeTable(base amqp.Table, overrides map[string]interface{}) amqp.Table {
	merged := amqp.Table{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	return merged
}

// buildRetryTopology builds the redelivery chain.
//
// A failed message is published as a copy to the stage queue of the attempt
// number, and that copy carries the remaining cumulative delay: the stage
// expires it back into the worker queue. Because the delay of attempt N is its
// cumulative wait, a stage queue holds the DELTA to the next stage, and the
// chain reproduces the configured per-attempt waiting times without ever
// delaying a message twice.
//
// The worker queue dead letters to the retry queue, which is the safety net for
// a message the server redelivers on its own: a copy parked there waits the
// retry TTL and is then redelivered through the same chain.
//
// Every exchange hop uses the worker queue name as the exchange and the name of
// the destination queue as the routing key, and each queue is bound by exactly
// that key, so a message reaches the queue it was addressed to and nothing else.
func buildRetryTopology(worker WorkerI, channel topologyChannel) (err error) {
	queue, durable := worker.GetQueue(), worker.GetDurable()
	failed, retry := worker.GetFailedQueue(), worker.GetRetryQueue()
	if err = channel.ExchangeDeclare(queue, "direct", durable, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange %s declare: %w", queue, err)
	}
	// The worker queue is bound under its own name for the last hop of the
	// chain. It is not bound with "#", because a queue-named exchange that
	// matched every routing key would swallow the copies addressed to the
	// other stages.
	if err = channel.QueueBind(queue, queue, queue, false, nil); err != nil {
		return fmt.Errorf("queue %s bind: %w", queue, err)
	}
	if err = channel.QueueBind(failed, failed, queue, false, nil); err != nil {
		return fmt.Errorf("queue %s bind: %w", failed, err)
	}
	delays := retryDelays(worker)
	if len(delays) == 0 {
		// Without a configured step there is no chain, so the retry queue is
		// not declared and the worker queue dead letters a rejected message
		// straight into the failed queue.
		return nil
	}
	if _, err = channel.QueueDeclare(
		retry,
		durable,
		false,
		false,
		false,
		amqp.Table{"x-dead-letter-exchange": queue, "x-dead-letter-routing-key": queue},
	); err != nil {
		return fmt.Errorf("queue %s declare: %w", retry, err)
	}
	if err = channel.QueueBind(retry, retry, queue, false, nil); err != nil {
		return fmt.Errorf("queue %s bind: %w", retry, err)
	}
	stages := make([]stage, 0, len(delays))
	for index, delay := range delays {
		stages = append(stages, stage{delay: delay, queue: fmt.Sprintf("%s.retry.%d", queue, index+1)})
	}
	stages = append(stages, stage{queue: failed})
	for index, current := range stages {
		if current.queue == failed {
			break
		}
		next := stages[index+1]
		ttl := current.delay
		if next.queue != failed && next.delay > current.delay {
			ttl = next.delay - current.delay
		}
		if ttl < time.Millisecond {
			ttl = time.Millisecond
		}
		if _, err = channel.QueueDeclare(
			current.queue,
			durable,
			false,
			false,
			false,
			amqp.Table{
				"x-dead-letter-exchange":    queue,
				"x-dead-letter-routing-key": next.queue,
				"x-message-ttl":             int32(ttl / time.Millisecond),
			},
		); err != nil {
			return fmt.Errorf("queue %s declare: %w", current.queue, err)
		}
		if err = channel.QueueBind(current.queue, current.queue, queue, false, nil); err != nil {
			return fmt.Errorf("queue %s bind: %w", current.queue, err)
		}
	}
	return nil
}

// stage is one hop of the redelivery chain. Its delay is the cumulative waiting
// time of the attempt it serves, and the failed queue terminates the chain with
// no delay of its own.
type stage struct {
	delay time.Duration
	queue string
}

// retryDelays normalizes the configured retry steps into the cumulative waiting
// time of each attempt, in the order they were configured. A step that is not a
// duration is skipped, and a sub-millisecond wait is raised to the 1ms floor the
// protocol can express.
func retryDelays(worker WorkerI) []time.Duration {
	var delays []time.Duration
	for _, step := range worker.GetSteps() {
		if step == "" {
			continue
		}
		delay, ok := parseDuration(step)
		if !ok {
			log.Println("Worker ", worker.GetName(), " retry step ", step, " is not a duration, step skipped")
			continue
		}
		if delay < time.Millisecond {
			delay = time.Millisecond
		}
		delays = append(delays, delay)
	}
	return delays
}

func consume(worker WorkerI, channel consumeChannel) (<-chan amqp.Delivery, error) {
	if prefetch := worker.PrefetchCount(); prefetch > 0 {
		if err := channel.Qos(prefetch, 0, false); err != nil {
			return nil, fmt.Errorf("channel qos: %w", err)
		}
	}
	tag := fmt.Sprintf("%s-%d", consumerTag(worker.GetName()), atomicNextConsumerSequence())
	return channel.Consume(worker.GetQueue(), tag, false, false, false, false, nil)
}

func consumeMessages(worker WorkerI, channel consumeChannel) {
	msgs, err := consume(worker, channel)
	if err != nil {
		log.Println("Consume error: ", err)
		return
	}
	for d := range msgs {
		process(worker, channel, &d)
	}
}

// topologyChannel is the part of *amqp.Channel the topology declaration uses.
type topologyChannel interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(queue, key, exchange string, noWait bool, args amqp.Table) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
}

// consumeChannel is the part of *amqp.Channel consuming needs: it settles the
// deliveries it reads, so it carries the acknowledgement surface too.
type consumeChannel interface {
	deliveryChannel
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

var (
	_ topologyChannel = (*amqp.Channel)(nil)
	_ consumeChannel  = (*amqp.Channel)(nil)
)

// deliveryChannel is the part of *amqp.Channel the acknowledgement decisions
// use. The decisions are the message safety contract, so they are stated
// against this narrow interface and verified with a fake channel instead of a
// broker.
type deliveryChannel interface {
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Tx() error
	TxCommit() error
	TxRollback() error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

var _ deliveryChannel = (*amqp.Channel)(nil)

// process runs one delivery and settles it exactly once: a processed message is
// acknowledged, an unprocessed one moves to the retry topology, which removes
// the original together with the copy it parked, or is requeued.
func process(worker WorkerI, channel deliveryChannel, d *amqp.Delivery) {
	exception := Exception{}
	err := excute(worker, &d.Body, &exception)
	if err == nil && exception.Msg == "" {
		acknowledge(worker, channel, d)
		return
	}
	// A recovered panic is the cause of the failure: the unwinding discarded
	// the error the worker returned, so only the exception survives.
	cause := err
	if exception.Msg != "" {
		cause = errors.New(exception.Msg)
	}
	// retry removes the original itself, in the same transaction as the copy it
	// parked, so nothing is settled twice here.
	if retry(worker, channel, d, cause) {
		return
	}
	// The message was neither processed nor parked: redeliver it instead of
	// losing it, because an ack would drop it and a plain ack-less return
	// withholds it until the consumer goes away.
	if !requeue(worker, channel, d) {
		log.Println("ERROR: ", worker.GetName(), " queue ", worker.GetQueue(), " message ", d.DeliveryTag, " failed and was not requeued: ", cause)
	}
}

// acknowledge removes a delivery. "ack: false" keeps the delivery on the
// server, so a crashed consumer cannot drop a processed message.
//
// The ack is always single: AckMultiple is offered as a configuration but is
// only safe when exactly one goroutine settles the channel, and the base
// delivery API settles per delivery tag anyway.
func acknowledge(worker WorkerI, channel deliveryChannel, d *amqp.Delivery) {
	if !worker.AckEnabled() {
		return
	}
	if err := channel.Ack(d.DeliveryTag, false); err != nil {
		log.Println("Ack error: ", err)
	}
}

func requeue(worker WorkerI, channel deliveryChannel, d *amqp.Delivery) bool {
	if !worker.RequeueEnabled() {
		return false
	}
	if err := channel.Nack(d.DeliveryTag, false, true); err != nil {
		log.Println("Requeue error: ", err)
		return false
	}
	return true
}

// retry moves a failed message into the redelivery topology: the copy is
// addressed to the stage of its attempt number, and that stage applies the
// remaining cumulative delay before redelivering it through the chain. A message
// that exhausted its budget is parked in the failed queue instead. It reports
// whether the delivery was settled, so its caller never settles it twice.
//
// The publish is done in a transaction, so the copy and the removal of the
// original are committed together: a connection loss cannot park a message
// while its original keeps coming back, or drop it while its copy was never
// made.
func retry(worker WorkerI, channel deliveryChannel, d *amqp.Delivery, cause error) bool {
	if !worker.RetryEnabled() {
		return false
	}
	delays := retryDelays(worker)
	attempted := retryCount(d)
	if attempted >= worker.MaxRetry() || len(delays) == 0 {
		if err := parkInFailedQueue(worker, channel, d, cause); err == nil {
			return true
		} else if errors.Is(err, errIndeterminate) {
			log.Println("Failed queue publish outcome unknown, leaving the message unacknowledged: ", err)
			return true
		} else {
			log.Println("Failed queue publish error: ", err)
		}
		return false
	}
	// The copy is addressed to the stage of this attempt, not to the worker
	// exchange, so the delay of that attempt is applied exactly once.
	stage := stageQueue(worker, min(attempted, len(delays)-1))
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["tryCount"] = int32(attempted + 1)
	headers["err"] = cause.Error()
	publish := func() error {
		return channel.PublishWithContext(
			context.Background(),
			worker.GetQueue(),
			stage,
			false,
			false,
			amqp.Publishing{
				Headers:      headers,
				ContentType:  d.ContentType,
				Body:         d.Body,
				DeliveryMode: amqp.Persistent,
				Priority:     d.Priority,
			},
		)
	}
	err := publishThenAck(channel, publish, d.DeliveryTag)
	if err == nil {
		return true
	}
	if errors.Is(err, errIndeterminate) {
		// The commit itself is unanswerable: the copy and the removal may already
		// be applied. Settling the tag again would be rejected as an unknown tag
		// or would requeue a message that is already parked, so the delivery is
		// left unacknowledged and the server decides.
		log.Println("Retry commit outcome unknown, leaving the message unacknowledged: ", err)
		return true
	}
	log.Println("Retry publish error: ", err)
	return false
}

func stageQueue(worker WorkerI, index int) string {
	return fmt.Sprintf("%s.retry.%d", worker.GetQueue(), index+1)
}

// errIndeterminate marks a transaction whose commit could not be answered. The
// broker may or may not have applied it, so a caller must not settle the
// delivery again on its account.
var errIndeterminate = errors.New("transaction outcome unknown")

// transactionLocks serializes the transactional section per channel. A
// transaction in AMQP is a property of the channel, not of a delivery: a
// rollback discards every uncommitted publish and ack of that channel, so two
// deliveries that park their copies on one channel at the same time can discard
// each other's copy and then acknowledge an original whose copy is gone. The
// critical section is held for the whole transaction, which is what makes the
// publish and its ack atomic for the message rather than for the channel.
var transactionLocks sync.Map

func transactionLock(channel deliveryChannel) *sync.Mutex {
	lock, _ := transactionLocks.LoadOrStore(channel, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// publishThenAck publishes a replacement and removes the original delivery in
// one transaction, so both are committed together: a connection loss can
// neither park a message while its original keeps coming back nor drop it while
// its copy was never made.
func publishThenAck(channel deliveryChannel, publish func() error, tag uint64) error {
	// The *amqp.Channel pointer is the lock key, and the interface is comparable
	// for every real channel and for the fake used in the tests.
	lock := transactionLock(channel)
	lock.Lock()
	defer lock.Unlock()

	if err := channel.Tx(); err != nil {
		return fmt.Errorf("transaction open: %w", err)
	}
	err := publish()
	if err == nil {
		err = channel.Ack(tag, false)
	}
	if err != nil {
		_ = channel.TxRollback()
		return err
	}
	if err = channel.TxCommit(); err != nil {
		return fmt.Errorf("%w: %v", errIndeterminate, err)
	}
	return nil
}

// parkInFailedQueue stores an exhausted message, carrying the attempt count
// and the last error, in the failed queue of the worker. The copy and the
// removal of the original are committed together, so a message can never be
// lost between the two queues and can never be left behind twice.
func parkInFailedQueue(worker WorkerI, channel deliveryChannel, d *amqp.Delivery, cause error) error {
	if _, err := channel.QueueDeclare(worker.GetFailedQueue(), worker.GetDurable(), false, false, false, nil); err != nil {
		return err
	}
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["tryCount"] = int32(retryCount(d))
	if cause != nil {
		headers["err"] = cause.Error()
	}
	return publishThenAck(channel, func() error {
		return channel.PublishWithContext(
			context.Background(),
			"",
			worker.GetFailedQueue(),
			false,
			false,
			amqp.Publishing{
				Headers:      headers,
				ContentType:  d.ContentType,
				Body:         d.Body,
				DeliveryMode: amqp.Persistent,
				Priority:     d.Priority,
			},
		)
	}, d.DeliveryTag)
}

// retryCount reads how many processing attempts a delivery already had. Only
// the header type amqp091-go decodes for int32 is trusted; a header an
// application published as another type is counted as the first attempt
// instead of panicking the consumer loop.
func retryCount(d *amqp.Delivery) int {
	count, ok := d.Headers["tryCount"]
	if !ok {
		return 0
	}
	switch value := count.(type) {
	case int32:
		return int(value)
	case int64:
		return int(value)
	case int:
		return value
	case string:
		parsed, err := strconv.Atoi(value)
		if err != nil {
			log.Println("tryCount header ", value, " is not a number, treated as the first attempt")
			return 0
		}
		return parsed
	}
	log.Println("tryCount header has unsupported type, treated as the first attempt")
	return 0
}

func consumerTag(name string) string {
	if name == "" {
		return "sneaker-go"
	}
	return name
}

func excute(worker WorkerI, body *[]byte, exception *Exception) (err error) {
	defer func(e *Exception) {
		r := recover()
		if r != nil {
			e.Msg = fmt.Sprintf("%v", r)
		}
	}(exception)
	err = worker.Work(body)
	return
}
