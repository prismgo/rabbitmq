# PrismGo RabbitMQ Extension

`github.com/prismgo/rabbitmq` adds the `rabbitmq` queue driver to one PrismGo Application. The transport supports publisher confirms, reconnect recovery, topology declaration, delayed delivery, consumer intent recovery, queue restart signals, and PrismGo queue lifecycle events.

## Installation

```bash
go get github.com/prismgo/rabbitmq
```

Register the extension between PrismGo's default providers and your application providers:

```go
import (
    qcontract "github.com/prismgo/framework/contracts/queue"
    "github.com/prismgo/framework/foundation"
    "github.com/prismgo/framework/queue"
    "github.com/prismgo/rabbitmq"
)

app := foundation.Configure().
    WithExtensionProviders(rabbitmq.ServiceProvider{}).
    WithProviders(applicationProviders...).
    Create()
```

The provider installs an application-local connector during `Boot`. It does not open an AMQP connection until the application's queue manager first resolves a RabbitMQ connection.

Your regular `queue.connections.rabbitmq` configuration continues to use `driver: "rabbitmq"`. Existing public error sentinels remain available from `github.com/prismgo/framework/queue`, and the extension exports the same RabbitMQ-specific sentinels for low-level users.

## Direct manager use

Tests and tools that construct a queue manager without an Application can install the connector directly:

```go
m, err := queue.NewManager(cfg, queue.NewRegistry())
if err != nil {
    return err
}
m.Extend("rabbitmq", func() (qcontract.Connector, error) {
    return rabbitmq.Connector{}, nil
})
```

## Integration tests

Set `PRISMGO_RABBITMQ_TEST_URL` to run the real broker gates:

```bash
PRISMGO_RABBITMQ_TEST_URL=amqp://guest:guest@127.0.0.1:5672/ \
    go test -race -coverprofile=coverage.out ./...
```
