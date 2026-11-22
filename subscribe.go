package sneaker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SubscribeMessageByQueue 声明拓扑并启动一个串行消费者；确认发生在处理完成之后。
func SubscribeMessageByQueue(worker WorkerI, arguments amqp.Table) (err error) {
	channel := worker.GetChannel()
	if channel == nil {
		return fmt.Errorf("channel for queue %s is unavailable", worker.GetQueue())
	}
	settlement, msgs, err := prepareSubscription(worker, channel, arguments)
	if err != nil {
		return err
	}
	go func() {
		defer releaseSettlement(channel, settlement)
		consumeMessages(worker, settlement, msgs)
	}()
	return
}

// SubscribeMessages 声明拓扑并并发消费，并发数由 GetThreads 或 GOMAXPROCS 限制。
func SubscribeMessages(worker WorkerI, arguments amqp.Table) (err error) {
	channel := worker.GetChannel()
	if channel == nil {
		return fmt.Errorf("channel for queue %s is unavailable", worker.GetQueue())
	}
	settlement, msgs, err := prepareSubscription(worker, channel, arguments)
	if err != nil {
		return err
	}
	go func() {
		defer releaseSettlement(channel, settlement)
		processMessages(worker, settlement, msgs)
	}()
	return
}

// subscriptionChannel 汇总声明拓扑和创建消费者所需的最小接口。
type subscriptionChannel interface {
	topologyChannel
	consumeChannel
}

// prepareSubscription 在结算锁内声明拓扑并启动消费者，避免与事务交错。
func prepareSubscription(worker WorkerI, channel subscriptionChannel, arguments amqp.Table) (*settlement, <-chan amqp.Delivery, error) {
	settlement := acquireSettlement(channel)
	var msgs <-chan amqp.Delivery
	err := settlement.with(func() error {
		if err := buildTopology(worker, channel, arguments); err != nil {
			return err
		}
		var consumeErr error
		msgs, consumeErr = consume(worker, channel)
		if consumeErr != nil {
			return fmt.Errorf("consume queue %s: %w", worker.GetQueue(), consumeErr)
		}
		return nil
	})
	if err != nil {
		releaseSettlement(channel, settlement)
		return nil, nil, err
	}
	return settlement, msgs, nil
}

// buildTopology 声明 worker 队列、失败队列、Exchange 绑定和重试链。
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

// mergeTable 合并 AMQP 参数表，overrides 覆盖 base 中的同名键。
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

// buildRetryTopology 声明重试交换机、重试队列和各阶段延迟队列。
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

// retryDelays 将配置的 steps 规范化为每次尝试的累计等待时间。
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

// consume 设置 QoS 并创建 broker 消费者。
func consume(worker WorkerI, channel consumeChannel) (<-chan amqp.Delivery, error) {
	if prefetch := worker.PrefetchCount(); prefetch > 0 {
		if err := channel.Qos(prefetch, 0, false); err != nil {
			return nil, fmt.Errorf("channel qos: %w", err)
		}
	}
	tag := fmt.Sprintf("%s-%d", consumerTag(worker.GetName()), atomicNextConsumerSequence())
	return channel.Consume(worker.GetQueue(), tag, false, false, false, false, nil)
}

// consumeMessages 串行处理消息，直到投递 channel 关闭。
func consumeMessages(worker WorkerI, settlement *settlement, msgs <-chan amqp.Delivery) {
	for d := range msgs {
		processDelivery(worker, settlement, &d)
	}
}

// processMessages 使用有界并发处理消息，并等待在途任务结束。
func processMessages(worker WorkerI, settlement *settlement, messages <-chan amqp.Delivery) {
	limit := worker.GetThreads()
	if limit <= 0 {
		limit = runtime.GOMAXPROCS(0)
	}
	semaphore := make(chan struct{}, limit)
	var group sync.WaitGroup
	for delivery := range messages {
		semaphore <- struct{}{}
		group.Add(1)
		go func(delivery amqp.Delivery) {
			defer group.Done()
			defer func() { <-semaphore }()
			processDelivery(worker, settlement, &delivery)
		}(delivery)
	}
	group.Wait()
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
	IsClosed() bool
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Tx() error
	TxCommit() error
	TxRollback() error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

var _ deliveryChannel = (*amqp.Channel)(nil)

// process 处理单条消息，并在结束后归还结算状态。
func process(worker WorkerI, channel deliveryChannel, d *amqp.Delivery) {
	settlement := acquireSettlement(channel)
	defer releaseSettlement(channel, settlement)
	processDelivery(worker, settlement, d)
}

// processDelivery 处理一条投递并保证只结算一次：成功 ack，失败重试或 requeue。
func processDelivery(worker WorkerI, settlement *settlement, d *amqp.Delivery) {
	exception := Exception{}
	err := excute(worker, &d.Body, &exception)
	if err == nil && exception.Msg == "" {
		acknowledge(worker, settlement, d)
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
	if retry(worker, settlement, d, cause) {
		return
	}
	// The message was neither processed nor parked: redeliver it instead of
	// losing it, because an ack would drop it and a plain ack-less return
	// withholds it until the consumer goes away.
	if !requeue(worker, settlement, d) {
		log.Println("ERROR: ", worker.GetName(), " queue ", worker.GetQueue(), " message ", d.DeliveryTag, " failed and was not requeued: ", cause)
	}
}

// acknowledge 根据 ack 配置确认一条已成功处理的消息。
func acknowledge(worker WorkerI, settlement *settlement, d *amqp.Delivery) {
	if !worker.AckEnabled() {
		return
	}
	if err := settlement.ack(d.DeliveryTag); err != nil {
		log.Println("Ack error: ", err)
	}
}

// requeue 根据 requeue 配置将未处理消息重新入队。
func requeue(worker WorkerI, settlement *settlement, d *amqp.Delivery) bool {
	if !worker.RequeueEnabled() {
		return false
	}
	if err := settlement.nack(d.DeliveryTag, true); err != nil {
		log.Println("Requeue error: ", err)
		return false
	}
	return true
}

// retry 将失败消息送入重试链或失败队列，并返回是否已完成结算。
func retry(worker WorkerI, settlement *settlement, d *amqp.Delivery, cause error) bool {
	if !worker.RetryEnabled() {
		return false
	}
	delays := retryDelays(worker)
	attempted := retryCount(d)
	if attempted >= worker.MaxRetry() || len(delays) == 0 {
		if err := parkInFailedQueue(worker, settlement, d, cause); err == nil {
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
		return settlement.channel.PublishWithContext(
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
	err := settlement.publishThenAck(publish, d.DeliveryTag)
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

// stageQueue 返回第 index 次重试对应的阶段队列名。
func stageQueue(worker WorkerI, index int) string {
	return fmt.Sprintf("%s.retry.%d", worker.GetQueue(), index+1)
}

// errIndeterminate marks a transaction whose commit could not be answered. The
// broker may or may not have applied it, so a caller must not settle the
// delivery again on its account.
var errIndeterminate = errors.New("transaction outcome unknown")

// settlement serializes every acknowledgement and transaction on one consumer
// channel. A transaction in AMQP is a property of the channel, not of a
// delivery: an ack issued by another goroutine while a transaction is open
// joins that transaction, and a rollback then discards it as well.
//
// A channel also stays in transaction mode after TxCommit. Once a retry has
// selected transactions, every later ack and nack must therefore be committed
// too; otherwise a successful delivery is left unacknowledged until the channel
// goes away and the broker redelivers it.
type settlement struct {
	channel       deliveryChannel
	mutex         sync.Mutex
	transactional bool
}

// newSettlement 为指定 Channel 创建结算状态。
func newSettlement(channel deliveryChannel) *settlement {
	return &settlement{channel: channel}
}

// settlementRegistry shares one settlement between all subscriptions that use
// the same channel. A channel is channel-wide state, so two subscriptions on it
// must not settle independently. Entries are released when the last consumer
// stops; a channel that has selected transactions is retained until it is
// closed, because a later subscription on that channel would still be in
// transaction mode.
type settlementEntry struct {
	settlement *settlement
	refs       int
}

var settlementRegistry = struct {
	sync.Mutex
	entries map[deliveryChannel]*settlementEntry
}{entries: make(map[deliveryChannel]*settlementEntry)}

// acquireSettlement 获取或创建 Channel 对应的共享结算状态，并增加引用计数。
func acquireSettlement(channel deliveryChannel) *settlement {
	settlementRegistry.Lock()
	defer settlementRegistry.Unlock()

	for existingChannel, entry := range settlementRegistry.entries {
		if entry.refs == 0 && existingChannel.IsClosed() {
			delete(settlementRegistry.entries, existingChannel)
		}
	}
	entry := settlementRegistry.entries[channel]
	if entry == nil {
		entry = &settlementEntry{settlement: newSettlement(channel)}
		settlementRegistry.entries[channel] = entry
	}
	entry.refs++
	return entry.settlement
}

// releaseSettlement 减少引用计数，并在不再需要时清理结算状态。
func releaseSettlement(channel deliveryChannel, used *settlement) {
	settlementRegistry.Lock()
	defer settlementRegistry.Unlock()

	entry := settlementRegistry.entries[channel]
	if entry == nil || entry.settlement != used {
		return
	}
	entry.refs--
	if entry.refs <= 0 && (!used.isTransactional() || channel.IsClosed()) {
		delete(settlementRegistry.entries, channel)
	}
}

// with 在结算互斥锁内执行指定操作。
func (s *settlement) with(fn func() error) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return fn()
}

// isTransactional 返回 Channel 是否已进入事务模式。
func (s *settlement) isTransactional() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.transactional
}

// ack 确认消息；事务模式下同时提交事务。
func (s *settlement) ack(tag uint64) error {
	return s.with(func() error {
		if err := s.channel.Ack(tag, false); err != nil {
			return err
		}
		return s.commitIfTransactional()
	})
}

// nack 拒绝消息；事务模式下同时提交事务。
func (s *settlement) nack(tag uint64, requeue bool) error {
	return s.with(func() error {
		if err := s.channel.Nack(tag, false, requeue); err != nil {
			return err
		}
		return s.commitIfTransactional()
	})
}

// commitIfTransactional 在事务模式下提交当前事务。
func (s *settlement) commitIfTransactional() error {
	if !s.transactional {
		return nil
	}
	if err := s.channel.TxCommit(); err != nil {
		return fmt.Errorf("%w: %v", errIndeterminate, err)
	}
	return nil
}

// publishThenAck 为单条投递执行“发布副本后确认原消息”的事务。
func publishThenAck(channel deliveryChannel, publish func() error, tag uint64) error {
	settlement := acquireSettlement(channel)
	defer releaseSettlement(channel, settlement)
	return settlement.publishThenAck(publish, tag)
}

// publishThenAck 在结算锁内发布副本并确认原消息。
func (s *settlement) publishThenAck(publish func() error, tag uint64) error {
	return s.with(func() error {
		return s.publishThenAckLocked(publish, tag)
	})
}

// publishThenAckLocked 在已持有结算锁时执行发布、确认和事务提交。
func (s *settlement) publishThenAckLocked(publish func() error, tag uint64) error {
	if !s.transactional {
		if err := s.channel.Tx(); err != nil {
			return fmt.Errorf("transaction open: %w", err)
		}
		s.transactional = true
	}
	err := publish()
	if err == nil {
		err = s.channel.Ack(tag, false)
	}
	if err != nil {
		_ = s.channel.TxRollback()
		return err
	}
	if err = s.channel.TxCommit(); err != nil {
		return fmt.Errorf("%w: %v", errIndeterminate, err)
	}
	return nil
}

// parkInFailedQueue 将重试耗尽的消息写入失败队列。
func parkInFailedQueue(worker WorkerI, settlement *settlement, d *amqp.Delivery, cause error) error {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["tryCount"] = int32(retryCount(d))
	if cause != nil {
		headers["err"] = cause.Error()
	}
	return settlement.with(func() error {
		if _, err := settlement.channel.QueueDeclare(worker.GetFailedQueue(), worker.GetDurable(), false, false, false, nil); err != nil {
			return err
		}
		return settlement.publishThenAckLocked(func() error {
			return settlement.channel.PublishWithContext(
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
	})
}

// retryCount 读取消息已重试次数，非法值按首次尝试处理。
func retryCount(d *amqp.Delivery) int {
	count, ok := d.Headers["tryCount"]
	if !ok {
		return 0
	}
	switch value := count.(type) {
	case int32:
		if value < 0 {
			return 0
		}
		return int(value)
	case int64:
		if value < 0 {
			return 0
		}
		return int(value)
	case int:
		if value < 0 {
			return 0
		}
		return value
	case string:
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			log.Println("tryCount header ", value, " is not a non-negative number, treated as the first attempt")
			return 0
		}
		return parsed
	}
	log.Println("tryCount header has unsupported type, treated as the first attempt")
	return 0
}

// consumerTag 返回消费者标签，空名称使用默认前缀。
func consumerTag(name string) string {
	if name == "" {
		return "sneaker-go"
	}
	return name
}

// excute 执行 worker 的 Work 方法并捕获 panic。
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
