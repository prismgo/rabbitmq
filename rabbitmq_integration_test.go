package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	qcontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

const rabbitMQIntegrationEnv = "PRISMGO_RABBITMQ_TEST_URL"

var integrationLog = struct {
	sync.Mutex
	hits map[string]int
}{hits: map[string]int{}}

type integrationJob struct {
	Key string `json:"key"`
}

func (j *integrationJob) Handle(context.Context) error {
	integrationLog.Lock()
	defer integrationLog.Unlock()
	integrationLog.hits[j.Key]++
	return nil
}

func TestRabbitMQIntegrationDispatchConsume(t *testing.T) {
	ctx := context.Background()
	fixture := newRabbitMQIntegrationFixture(t, "dispatch")
	manager := fixture.newManager(t)

	resetIntegrationLog()
	jobKey := fixture.name + "-job"
	if _, err := queue.NewDispatcher(manager).Dispatch(ctx, &integrationJob{Key: jobKey}); err != nil {
		t.Fatalf("dispatch rabbitmq job: %v", err)
	}
	if err := queue.NewWorker(manager).Work(ctx, queue.WorkerOptions{
		Connection: "rabbitmq",
		Queues:     []string{fixture.queue},
		Once:       true,
	}); err != nil {
		t.Fatalf("work rabbitmq job: %v", err)
	}
	if got := integrationJobHits(jobKey); got != 1 {
		t.Fatalf("job hits = %d, want 1", got)
	}
}

func TestRabbitMQIntegrationDelayRelease(t *testing.T) {
	ctx := context.Background()
	fixture := newRabbitMQIntegrationFixture(t, "release")
	manager := fixture.newManager(t)
	conn, err := manager.Queue("rabbitmq")
	if err != nil {
		t.Fatalf("rabbitmq connection: %v", err)
	}

	if _, err := queue.NewDispatcher(manager).Dispatch(ctx, &integrationJob{Key: fixture.name + "-release"}); err != nil {
		t.Fatalf("dispatch rabbitmq job: %v", err)
	}
	reserved, err := conn.Pop(ctx, []string{fixture.queue})
	if err != nil {
		t.Fatalf("pop rabbitmq job: %v", err)
	}
	if err := reserved.Release(ctx, 200*time.Millisecond); err != nil {
		t.Fatalf("release rabbitmq job: %v", err)
	}
	if _, err := conn.Pop(ctx, []string{fixture.queue}, qcontract.PopNoWait); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("immediate pop after delayed release err = %v, want ErrEmpty", err)
	}

	released, err := conn.Pop(ctx, []string{fixture.queue})
	if err != nil {
		t.Fatalf("pop released rabbitmq job: %v", err)
	}
	if released.Attempts() < 2 {
		t.Fatalf("released attempts = %d, want at least 2", released.Attempts())
	}
	if err := released.Delete(ctx); err != nil {
		t.Fatalf("delete released rabbitmq job: %v", err)
	}
}

func TestRabbitMQIntegrationPublicAMQPAdapters(t *testing.T) {
	url := strings.TrimSpace(os.Getenv(rabbitMQIntegrationEnv))
	if url == "" {
		t.Skipf("%s is not set; skipping real RabbitMQ integration test", rabbitMQIntegrationEnv)
	}
	conn, err := DefaultDial(url, amqp.Config{})
	if err != nil {
		t.Fatalf("dial rabbitmq adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close rabbitmq adapter connection: %v", err)
		}
	})
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open rabbitmq adapter channel: %v", err)
	}
	t.Cleanup(func() {
		if err := ch.Close(); err != nil {
			t.Errorf("close rabbitmq adapter channel: %v", err)
		}
	})
	queueName := fmt.Sprintf("prismgo.integration.adapter.%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(queueName, false, true, true, false, nil); err != nil {
		t.Fatalf("declare rabbitmq adapter queue: %v", err)
	}
	if _, err := ch.QueueDeclarePassive(queueName, false, true, true, false, nil); err != nil {
		t.Fatalf("passive declare rabbitmq adapter queue: %v", err)
	}
}

type rabbitMQIntegrationFixture struct {
	url          string
	name         string
	exchange     string
	queue        string
	restartQueue string
}

func newRabbitMQIntegrationFixture(t *testing.T, slug string) rabbitMQIntegrationFixture {
	t.Helper()
	url := strings.TrimSpace(os.Getenv(rabbitMQIntegrationEnv))
	if url == "" {
		t.Skipf("%s is not set; skipping real RabbitMQ integration test", rabbitMQIntegrationEnv)
	}
	name := fmt.Sprintf("prismgo.integration.%s.%d", slug, time.Now().UnixNano())
	fixture := rabbitMQIntegrationFixture{
		url:          url,
		name:         name,
		exchange:     name + ".exchange",
		queue:        name + ".queue",
		restartQueue: name + ".restart",
	}
	t.Cleanup(func() { fixture.cleanup(t) })
	return fixture
}

func (f rabbitMQIntegrationFixture) newManager(t *testing.T) *queue.Manager {
	t.Helper()
	registry := queue.NewRegistry()
	queue.RegisterTypeTo[*integrationJob](registry)
	manager, err := queue.NewManager(queue.Config{
		Default: "rabbitmq",
		Connections: map[string]queue.ConnectionConfig{
			"rabbitmq": {
				Driver:   "rabbitmq",
				Queue:    f.queue,
				BlockFor: 2 * time.Second,
				Options: map[string]any{
					"url":                f.url,
					"exchange":           f.exchange,
					"exchange_type":      "direct",
					"declare":            true,
					"exchange_durable":   false,
					"queue_durable":      false,
					"message_persistent": false,
					"confirm":            true,
					"delay_mode":         "ttl_dlx",
					"delay_buckets":      []time.Duration{200 * time.Millisecond},
					"prefetch":           1,
					"publish_timeout":    2 * time.Second,
					"restart_queue":      f.restartQueue,
					"restart_enabled":    true,
				},
			},
		},
	}, registry)
	if err != nil {
		t.Fatalf("new rabbitmq manager: %v", err)
	}
	manager.Extend("rabbitmq", func() (qcontract.Connector, error) {
		return Connector{}, nil
	})
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Logf("close rabbitmq manager: %v", err)
		}
	})
	return manager
}

func (f rabbitMQIntegrationFixture) cleanup(t *testing.T) {
	t.Helper()
	conn, err := amqp.Dial(f.url)
	if err != nil {
		t.Logf("dial rabbitmq for cleanup: %v", err)
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

	_, _ = ch.QueueDelete(f.queue, false, false, false)
	_, _ = ch.QueueDelete(f.exchange+"."+f.queue+".delay.0s", false, false, false)
	_, _ = ch.QueueDelete(f.restartQueue, false, false, false)
	_ = ch.ExchangeDelete(f.exchange, false, false)
}

func resetIntegrationLog() {
	integrationLog.Lock()
	defer integrationLog.Unlock()
	integrationLog.hits = map[string]int{}
}

func integrationJobHits(key string) int {
	integrationLog.Lock()
	defer integrationLog.Unlock()
	return integrationLog.hits[key]
}
