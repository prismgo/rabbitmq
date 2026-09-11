package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	qcontract "github.com/prismgo/framework/contracts/queue"
	fqueue "github.com/prismgo/framework/queue"
	qdriver "github.com/prismgo/framework/queue/driver"
	"github.com/prismgo/framework/queue/payload"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublicConnectionBoundaryErrors(t *testing.T) {
	opts := DefaultOptions()
	if opts.Host != DefaultHost || !opts.Declare.Or(false) || !opts.Confirm.Or(false) {
		t.Fatalf("DefaultOptions() = %#v, want documented host and enabled declare/confirm", opts)
	}
	if _, err := (Connector{}).Connect(context.Background(), "rabbit", qcontract.ConnectorConfig{RetryAfter: time.Second}); !errors.Is(err, fqueue.ErrUnsupportedRetryAfter) {
		t.Fatalf("Connector.Connect() error = %v, want %v", err, fqueue.ErrUnsupportedRetryAfter)
	}

	var conn *Connection
	if _, err := conn.Pop(context.Background(), nil, PopOptions{}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.Pop() error = %v, want %v", err, ErrConnectionClosed)
	}
	if err := conn.Push(context.Background(), "jobs", &payload.Envelope{}, 0); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.Push() error = %v, want %v", err, ErrConnectionClosed)
	}
	if _, err := conn.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{{Envelope: &payload.Envelope{}}}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.PushBulk() error = %v, want %v", err, ErrConnectionClosed)
	}
	if err := conn.Delete(context.Background(), nil); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.Delete() error = %v, want %v", err, ErrConnectionClosed)
	}
	if err := conn.Release(context.Background(), nil, 0); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.Release() error = %v, want %v", err, ErrConnectionClosed)
	}
	if err := conn.Clear(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.Clear() error = %v, want %v", err, ErrConnectionClosed)
	}
	if _, err := conn.Size(context.Background(), "jobs"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.Size() error = %v, want %v", err, ErrConnectionClosed)
	}
	if _, err := conn.AcquireConsumerIntent(nil); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.AcquireConsumerIntent() error = %v, want %v", err, ErrConnectionClosed)
	}
	if _, err := conn.RestartRequestedAt(context.Background()); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("nil Connection.RestartRequestedAt() error = %v, want %v", err, ErrConnectionClosed)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("nil Connection.Close() error = %v, want nil", err)
	}

	var queue *RabbitMQQueue
	if got, ok := queue.NewPopSession().(*RabbitMQQueue); !ok || got != nil {
		t.Fatalf("nil RabbitMQQueue.NewPopSession() = %#v (%T), want typed nil", got, got)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("nil RabbitMQQueue.Close() error = %v, want nil", err)
	}
}

func TestReconnectLoopRetriesDialAndRestoreBeforeReady(t *testing.T) {
	conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{
		Declare:           Bool(true),
		ReconnectMinDelay: time.Nanosecond,
		ReconnectMaxDelay: time.Nanosecond,
	})
	conn.reconnecting = true
	conn.reconnectLooping = true
	conn.ready = false
	conn.readyCh = make(chan struct{})
	conn.activeConsumers["jobs"] = struct{}{}
	conn.consumerRefs["jobs"] = 1

	dialErr := errors.New("dial failed")
	badConn := &rabbitMQTopologyTestConnection{channels: []AMQPChannel{
		&rabbitMQTopologyTestChannel{queueDeclareErr: errors.New("restore failed")},
	}}
	dialCalls := 0
	conn.options.Dialer = func(string, amqp.Config) (AMQPConnection, error) {
		dialCalls++
		switch dialCalls {
		case 1:
			return nil, dialErr
		case 2:
			return badConn, nil
		default:
			return &rabbitMQTopologyTestConnection{channels: []AMQPChannel{
				&rabbitMQTopologyTestChannel{},
			}}, nil
		}
	}
	var events []string
	qdriver.UseEventSink(func(_ context.Context, event qdriver.Event) {
		events = append(events, event.Name())
	})
	t.Cleanup(func() { qdriver.UseEventSink(nil) })

	conn.reconnectLoop()
	if got, want := dialCalls, 3; got != want {
		t.Fatalf("reconnect dial calls = %d, want %d", got, want)
	}
	if !badConn.closed {
		t.Fatal("failed restore connection remained open, want closed")
	}
	if conn.isReconnecting() {
		t.Fatal("Connection remained reconnecting after successful restore")
	}
	if err := conn.waitReady(context.Background()); err != nil {
		t.Fatalf("Connection.waitReady() error = %v, want nil", err)
	}
	if !hasRabbitMQEventName(events, qdriver.EventConnectionReconnectFailed) || !hasRabbitMQEventName(events, qdriver.EventConnectionReconnected) {
		t.Fatalf("reconnect events = %v, want failed and reconnected", events)
	}
}
