package rabbitmq

import (
	"errors"
	"strings"
	"testing"

	qcontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/foundation"
	"github.com/prismgo/framework/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestServiceProviderRegistersLazyApplicationLocalConnector(t *testing.T) {
	dialErr := errors.New("test dial stopped")
	dialCalls := 0
	address := ""
	manager := newTestManager(t, map[string]any{
		"url": "amqp://guest:secret@rabbitmq.example:5673/tenant",
		"dialer": Dialer(func(got string, _ amqp.Config) (AMQPConnection, error) {
			dialCalls++
			address = got
			return nil, dialErr
		}),
	})
	otherResolverErr := errors.New("other application resolver")
	otherResolverCalls := 0
	otherManager := newTestManager(t, nil)
	otherManager.Extend("rabbitmq", func() (qcontract.Connector, error) {
		otherResolverCalls++
		return nil, otherResolverErr
	})
	otherApp := foundation.NewApplication()
	t.Cleanup(func() { _ = otherApp.Close() })
	if err := otherApp.Instance("queue.manager", otherManager); err != nil {
		t.Fatalf("bind other application queue manager: %v", err)
	}

	app := foundation.NewApplication()
	t.Cleanup(func() { _ = app.Close() })
	if err := app.Instance("queue.manager", manager); err != nil {
		t.Fatalf("bind queue manager: %v", err)
	}
	provider := ServiceProvider{}
	if got, want := provider.Name(), "prismgo.extension.rabbitmq"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
	if err := provider.Register(app); err != nil {
		t.Fatalf("Register() error = %v, want nil", err)
	}
	if err := provider.Boot(app); err != nil {
		t.Fatalf("Boot() error = %v, want nil", err)
	}
	if dialCalls != 0 {
		t.Fatalf("dial calls after Boot() = %d, want 0", dialCalls)
	}

	if _, err := otherManager.Queue("rabbit"); !errors.Is(err, otherResolverErr) {
		t.Fatalf("other application Queue() error = %v, want %v", err, otherResolverErr)
	}
	if otherResolverCalls != 1 {
		t.Fatalf("other application resolver calls = %d, want 1", otherResolverCalls)
	}
	if _, err := manager.Queue("rabbit"); !errors.Is(err, ErrRabbitMQDialFailed) || !strings.Contains(err.Error(), dialErr.Error()) {
		t.Fatalf("extension Queue() error = %v, want %v with cause text %q", err, ErrRabbitMQDialFailed, dialErr)
	}
	if dialCalls != 1 {
		t.Fatalf("dial calls after Queue() = %d, want 1", dialCalls)
	}
	if got, want := address, "amqp://guest:secret@rabbitmq.example:5673/tenant"; got != want {
		t.Fatalf("dial address = %q, want %q", got, want)
	}
}

func newTestManager(t *testing.T, options map[string]any) *queue.Manager {
	t.Helper()
	manager, err := queue.NewManager(queue.Config{
		Default: "rabbit",
		Connections: map[string]queue.ConnectionConfig{
			"rabbit": {Driver: "rabbitmq", Options: options},
		},
	}, queue.NewRegistry())
	if err != nil {
		t.Fatalf("NewManager() error = %v, want nil", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}
