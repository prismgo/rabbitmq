package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	qcontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/event"
	"github.com/prismgo/framework/foundation"
	"github.com/prismgo/framework/horizon"
	"github.com/prismgo/framework/kernel"
	"github.com/prismgo/framework/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

const horizonIntegrationRabbitMQEnv = "PRISMGO_RABBITMQ_TEST_URL"

func TestRabbitMQHorizonIntegrationGateSkipsUnsupportedFailedAndBatchState(t *testing.T) {
	// 需求背景：RabbitMQ driver 当前不承载 failed job 和 batch 持久状态，Horizon integration contract 要求这类
	// Prismgo driver 能力差异被明确记录，不能作为 Horizon 集成门失败条件。该测试只验证
	// RabbitMQ job 消费会进入 Horizon metrics/control/store 边界。
	ctx := context.Background()
	url := strings.TrimSpace(os.Getenv(horizonIntegrationRabbitMQEnv))
	if url == "" {
		t.Skipf("%s is not set; skipping RabbitMQ Horizon integration gate", horizonIntegrationRabbitMQEnv)
	}
	name := fmt.Sprintf("prismgo.horizon.horizon_integration.%d", time.Now().UnixNano())
	exchange := name + ".exchange"
	queueName := name + ".queue"
	restartQueue := name + ".restart"
	t.Cleanup(func() { cleanupHorizonIntegrationRabbitMQ(t, url, exchange, queueName, restartQueue) })

	queueManager, err := newHorizonIntegrationRabbitMQQueueManager(url, exchange, queueName, restartQueue)
	if err != nil {
		t.Fatalf("new rabbitmq queue manager: %v", err)
	}
	t.Cleanup(func() {
		if err := queueManager.Close(); err != nil {
			t.Logf("close rabbitmq queue manager: %v", err)
		}
	})

	app := foundation.NewApplication()
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Instance("queue.manager", queueManager); err != nil {
		t.Fatalf("bind queue manager: %v", err)
	}
	bus := event.New()
	if err := app.Instance("event.dispatcher", bus); err != nil {
		t.Fatalf("register event dispatcher: %v", err)
	}
	if err := (queue.ServiceProvider{}).Boot(app); err != nil {
		t.Fatalf("boot queue provider bridge: %v", err)
	}
	if err := (ServiceProvider{}).Boot(app); err != nil {
		t.Fatalf("boot rabbitmq extension provider: %v", err)
	}
	t.Cleanup(func() { queue.UseEventSink(nil) })

	store := horizon.NewMemoryStore(horizon.StoreOptions{Prefix: "horizon_integration_rabbitmq", HeartbeatTTL: time.Minute})
	manager, err := horizon.NewManager(horizonIntegrationConfig("memory", "rabbitmq", queueName),
		horizon.WithStoreFactory(horizonIntegrationStaticStore{store: store}),
		horizon.WithQueueManager(horizon.NewQueueAdapter(queueManager)),
		horizon.WithWorkerRunner(horizon.NewQueueWorkerAdapter(queueManager)),
		horizon.WithEventDispatcher(bus),
	)
	if err != nil {
		t.Fatalf("new rabbitmq horizon manager: %v", err)
	}
	if err := manager.RegisterMonitor(ctx); err != nil {
		t.Fatalf("register horizon monitor: %v", err)
	}

	if _, err := queue.NewDispatcher(queueManager).Dispatch(ctx, &horizonIntegrationJob{Value: "rabbitmq"}, queue.OnQueue(queueName)); err != nil {
		t.Fatalf("dispatch rabbitmq job: %v", err)
	}
	rabbitOpts := queue.WorkerOptions{
		Connection: "rabbitmq",
		Queues:     []string{queueName},
		Once:       true,
		Tries:      1,
	}
	rabbitSession, err := manager.WorkerRunner().Begin(ctx, rabbitOpts)
	if err != nil {
		t.Fatalf("begin rabbitmq worker session: %v", err)
	}
	defer func() {
		if err := rabbitSession.Close(); err != nil {
			t.Errorf("close rabbit session: %v", err)
		}
	}()
	if err := rabbitSession.Activate(ctx); err != nil {
		t.Fatalf("activate rabbitmq worker session: %v", err)
	}
	if err := rabbitSession.Work(ctx); err != nil {
		t.Fatalf("work rabbitmq job once: %v", err)
	}

	if err := app.Instance("horizon.manager", manager); err != nil {
		t.Fatalf("bind rabbitmq horizon manager: %v", err)
	}
	k := kernel.New("rabbitmq-horizon-integration")
	for _, factory := range horizon.CommandFactories() {
		k.Register(factory())
	}
	if err := k.CallSilently(ctx, "horizon:snapshot"); err != nil {
		t.Fatalf("snapshot rabbitmq integration: %v", err)
	}
	windows, err := store.EventMetricWindows(ctx, horizon.EventMetricWindowQuery{})
	if err != nil {
		t.Fatalf("read rabbitmq metrics: %v", err)
	}
	processed := int64(0)
	for _, window := range windows.Items {
		processed += window.Processed
	}
	lengths, err := store.QueueLengthSnapshot(ctx)
	if err != nil {
		t.Fatalf("read rabbitmq queue lengths: %v", err)
	}
	if processed != 1 || len(lengths.Queues) != 1 {
		t.Fatalf("rabbitmq snapshot = processed %d queue lengths %d, want 1 and 1", processed, len(lengths.Queues))
	}
	if err := k.CallSilently(ctx, "horizon:terminate"); err != nil {
		t.Fatalf("request rabbitmq horizon terminate: %v", err)
	}
}

type horizonIntegrationStaticStore struct {
	store horizon.Store
}

func (r horizonIntegrationStaticStore) ResolveStore(context.Context, horizon.Config) (horizon.Store, error) {
	return r.store, nil
}

type horizonIntegrationJob struct {
	Value string
}

func (j *horizonIntegrationJob) Handle(context.Context) error {
	return nil
}

func horizonIntegrationConfig(storeName, connection, queueName string) horizon.Config {
	return horizon.Config{
		Store:        storeName,
		Environment:  "local",
		HeartbeatTTL: time.Minute,
		Supervisors: map[string]horizon.SupervisorConfig{
			"supervisor-default": {
				Name:       "supervisor-default",
				Connection: connection,
				Queues:     []string{queueName},
			},
		},
	}
}

func newHorizonIntegrationRabbitMQQueueManager(url, exchange, queueName, restartQueue string) (*queue.Manager, error) {
	registry := queue.NewRegistry()
	queue.RegisterTypeTo[*horizonIntegrationJob](registry)
	manager, err := queue.NewManager(queue.Config{
		Default: "rabbitmq",
		Connections: map[string]queue.ConnectionConfig{
			"rabbitmq": {
				Driver:   "rabbitmq",
				Queue:    queueName,
				BlockFor: 2 * time.Second,
				Options: map[string]any{
					"url":                url,
					"exchange":           exchange,
					"exchange_type":      "direct",
					"declare":            true,
					"exchange_durable":   false,
					"queue_durable":      false,
					"message_persistent": false,
					"confirm":            true,
					"prefetch":           1,
					"publish_timeout":    2 * time.Second,
					"restart_queue":      restartQueue,
					"restart_enabled":    true,
				},
			},
		},
	}, registry)
	if err != nil {
		return nil, err
	}
	manager.Extend("rabbitmq", func() (qcontract.Connector, error) {
		return Connector{}, nil
	})
	return manager, nil
}

func cleanupHorizonIntegrationRabbitMQ(t *testing.T, url, exchange, queueName, restartQueue string) {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Logf("dial rabbitmq cleanup: %v", err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close rabbitmq connection: %v", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		t.Logf("open rabbitmq cleanup channel: %v", err)
		return
	}
	defer func() {
		if err := ch.Close(); err != nil {
			t.Errorf("close rabbitmq channel: %v", err)
		}
	}()
	_, _ = ch.QueueDelete(queueName, false, false, false)
	_, _ = ch.QueueDelete(restartQueue, false, false, false)
	_ = ch.ExchangeDelete(exchange, false, false)
}
