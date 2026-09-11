package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	qcontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/foundation"
	"github.com/prismgo/framework/queue/payload"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublicPublishContextAndFailureBoundaries(t *testing.T) {
	env := &payload.Envelope{ID: "publish-boundary", Name: "Job", Queue: "jobs", CreatedAt: 1, AvailableAt: 1}

	bestEffortChannel := &rabbitMQTopologyTestChannel{}
	bestEffort := newRabbitMQTopologyTestConnection(bestEffortChannel, Options{Declare: Bool(true), Confirm: Bool(false)})
	//nolint:staticcheck // A nil context is the public fallback contract under test.
	if err := bestEffort.Push(nil, "jobs", env, 0); err != nil {
		t.Fatalf("Push(nil context) error = %v, want nil", err)
	}
	//nolint:staticcheck // A nil context is the public fallback contract under test.
	if result, err := bestEffort.PushBulk(nil, "jobs", []rabbitMQBulkPublishItem{{Envelope: env}}); err != nil || result.Accepted != 1 {
		t.Fatalf("PushBulk(nil context) = (%#v, %v), want accepted 1 and nil", result, err)
	}

	pushTimeout := newRabbitMQTopologyTestConnection(
		&rabbitMQTopologyTestChannel{publishErr: context.DeadlineExceeded},
		Options{Declare: Bool(true), Confirm: Bool(false)},
	)
	if err := pushTimeout.Push(context.Background(), "jobs", env, 0); !errors.Is(err, ErrRabbitMQPublishTimeout) {
		t.Fatalf("Push() deadline error = %v, want %v", err, ErrRabbitMQPublishTimeout)
	}

	bulkTimeout := newRabbitMQTopologyTestConnection(
		&rabbitMQTopologyTestChannel{publishErr: context.DeadlineExceeded},
		Options{Declare: Bool(true), Confirm: Bool(false)},
	)
	if result, err := bulkTimeout.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{{Envelope: env}}); !errors.Is(err, ErrRabbitMQPublishTimeout) || result.Accepted != 0 {
		t.Fatalf("PushBulk() deadline = (%#v, %v), want accepted 0 and %v", result, err, ErrRabbitMQPublishTimeout)
	}

	waitTimeout := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{PublishTimeout: time.Millisecond})
	waitTimeout.ready = false
	waitTimeout.reconnecting = true
	waitTimeout.readyCh = make(chan struct{})
	if result, err := waitTimeout.PushBulk(context.Background(), "jobs", []rabbitMQBulkPublishItem{{Envelope: env}}); !errors.Is(err, ErrRabbitMQPublishTimeout) || result.Accepted != 0 {
		t.Fatalf("PushBulk() reconnect timeout = (%#v, %v), want accepted 0 and %v", result, err, ErrRabbitMQPublishTimeout)
	}
}

func TestPublicPopContextAndConsumerFailureBoundaries(t *testing.T) {
	consumeErr := errors.New("consume failed")
	conn := newRabbitMQTopologyTestConnection(
		&rabbitMQTopologyTestChannel{consumeErr: consumeErr},
		Options{Declare: Bool(true)},
	)
	//nolint:staticcheck // A nil context is the public fallback contract under test.
	if _, err := conn.Pop(nil, []string{"jobs"}, PopOptions{}); !errors.Is(err, consumeErr) {
		t.Fatalf("Pop(nil context) error = %v, want %v", err, consumeErr)
	}

	waiting := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	waiting.ready = false
	waiting.reconnecting = true
	waiting.readyCh = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waiting.Pop(ctx, []string{"jobs"}, PopOptions{BlockFor: time.Second}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Pop(canceled context) error = %v, want %v", err, context.Canceled)
	}

	closed := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	if err := closed.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if _, err := closed.Pop(context.Background(), []string{"jobs"}, PopOptions{}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed Pop() error = %v, want %v", err, ErrConnectionClosed)
	}
}

func TestPublicConnectorProviderCodecAndCloseFailures(t *testing.T) {
	app := foundation.NewApplication()
	t.Cleanup(func() { _ = app.Close() })
	if err := (ServiceProvider{}).Boot(app); err == nil {
		t.Fatal("ServiceProvider.Boot() error = nil, want missing queue manager error")
	}

	queue, err := (Connector{}).Connect(context.Background(), "rabbit", qcontract.ConnectorConfig{
		Options: map[string]any{
			"dialer": Dialer(func(string, amqp.Config) (AMQPConnection, error) {
				return &rabbitMQTopologyTestConnection{}, nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("Connector.Connect() error = %v, want nil", err)
	}
	if _, ok := queue.(*RabbitMQQueue); !ok {
		t.Fatalf("Connector.Connect() queue = %T, want *RabbitMQQueue", queue)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("connected Queue.Close() error = %v, want nil", err)
	}

	marshalErr := errors.New("marshal failed")
	codecConn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{Declare: Bool(true)})
	codecConn.codec = failingCodec{marshalErr: marshalErr}
	if err := codecConn.Push(context.Background(), "jobs", &payload.Envelope{}, 0); !errors.Is(err, marshalErr) {
		t.Fatalf("Connection.Push() codec error = %v, want %v", err, marshalErr)
	}

	closeErr := errors.New("close failed")
	closeCases := []struct {
		name  string
		setup func(*Connection)
	}{
		{name: "publish slot", setup: func(conn *Connection) {
			conn.publishSlots = []*rabbitMQPublishSlot{{channel: &rabbitMQTopologyTestChannel{closeErr: closeErr}}}
		}},
		{name: "topology channel", setup: func(conn *Connection) {
			conn.topologyChannel = &rabbitMQTopologyTestChannel{closeErr: closeErr}
		}},
		{name: "consumer channel", setup: func(conn *Connection) {
			conn.consumerChannel = &rabbitMQTopologyTestChannel{closeErr: closeErr}
		}},
		{name: "AMQP connection", setup: func(conn *Connection) {
			conn.amqpConnection = &closeErrorAMQPConnection{err: closeErr}
		}},
	}
	for _, tc := range closeCases {
		t.Run(tc.name, func(t *testing.T) {
			conn := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{})
			tc.setup(conn)
			if err := conn.Close(); !errors.Is(err, closeErr) {
				t.Fatalf("Connection.Close() error = %v, want %v", err, closeErr)
			}
		})
	}
}

func TestPublicCustomCodecDelayAndUntrackedDeliveryBoundaries(t *testing.T) {
	env := &payload.Envelope{ID: "custom-codec", Name: "Job", Queue: "jobs", CreatedAt: 1, AvailableAt: 1}
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{Declare: Bool(true), Confirm: Bool(false)})
	conn.codec = staticCodec{name: "json", body: []byte(`{"encoded":true}`)}
	if err := conn.Push(context.Background(), "jobs", env, 0); err != nil {
		t.Fatalf("Connection.Push() custom codec error = %v, want nil", err)
	}
	if len(channel.published) != 1 || channel.published[0].ContentType != "application/json" {
		t.Fatalf("published messages = %#v, want one application/json message", channel.published)
	}

	unsupported := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{
		Declare:   Bool(true),
		Confirm:   Bool(false),
		DelayMode: "unsupported",
	})
	if err := unsupported.Push(context.Background(), "jobs", env, time.Second); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("Connection.Push() unsupported delay error = %v, want %v", err, ErrUnsupportedOperation)
	}
	if err := conn.Delete(context.Background(), nil); err != nil {
		t.Fatalf("Connection.Delete(nil) error = %v, want nil", err)
	}
	if err := conn.Delete(context.Background(), &payload.Envelope{}); err != nil {
		t.Fatalf("Connection.Delete(untracked) error = %v, want nil", err)
	}
	if err := conn.Release(context.Background(), nil, 0); err == nil {
		t.Fatal("Connection.Release(nil) error = nil, want invalid envelope error")
	}
}

type failingCodec struct{ marshalErr error }

func (c failingCodec) Marshal(any) ([]byte, error) { return nil, c.marshalErr }
func (failingCodec) Unmarshal([]byte, any) error   { return nil }
func (failingCodec) Name() string                  { return "failing" }

type staticCodec struct {
	name string
	body []byte
}

func (c staticCodec) Marshal(any) ([]byte, error) { return append([]byte(nil), c.body...), nil }
func (staticCodec) Unmarshal([]byte, any) error   { return nil }
func (c staticCodec) Name() string                { return c.name }

type closeErrorAMQPConnection struct{ err error }

func (*closeErrorAMQPConnection) Channel() (AMQPChannel, error) {
	return nil, errors.New("channel unavailable")
}
func (*closeErrorAMQPConnection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error { return ch }
func (c *closeErrorAMQPConnection) Close() error                                   { return c.err }
func (*closeErrorAMQPConnection) IsClosed() bool                                   { return false }

func TestPublicRestartContextAndFailureBoundaries(t *testing.T) {
	channel := &rabbitMQTopologyTestChannel{}
	conn := newRabbitMQTopologyTestConnection(channel, Options{
		Declare:        Bool(true),
		Confirm:        Bool(false),
		RestartEnabled: Bool(true),
	})
	//nolint:staticcheck // A nil context is the public fallback contract under test.
	if err := conn.RequestRestart(nil, time.Time{}); err != nil {
		t.Fatalf("RequestRestart(nil context, zero time) error = %v, want nil", err)
	}
	if len(channel.published) != 1 || len(channel.published[0].Body) == 0 {
		t.Fatalf("restart publications = %#v, want one timestamp message", channel.published)
	}

	waiting := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{}, Options{
		Declare:             Bool(true),
		RestartEnabled:      Bool(true),
		PublishTimeout:      time.Millisecond,
		ReconnectMinDelay:   time.Millisecond,
		ReconnectMaxDelay:   time.Millisecond,
		RestartPollInterval: time.Millisecond,
	})
	waiting.ready = false
	waiting.reconnecting = true
	waiting.readyCh = make(chan struct{})
	if err := waiting.RequestRestart(context.Background(), time.Now()); !errors.Is(err, ErrRabbitMQPublishTimeout) {
		t.Fatalf("RequestRestart() reconnect timeout = %v, want %v", err, ErrRabbitMQPublishTimeout)
	}

	getErr := errors.New("restart get failed")
	readFailure := newRabbitMQTopologyTestConnection(&rabbitMQTopologyTestChannel{getErr: getErr}, Options{
		Declare:        Bool(true),
		RestartEnabled: Bool(true),
	})
	//nolint:staticcheck // A nil context is the public fallback contract under test.
	if _, err := readFailure.RestartRequestedAt(nil); !errors.Is(err, getErr) {
		t.Fatalf("RestartRequestedAt(nil context) error = %v, want %v", err, getErr)
	}
}
