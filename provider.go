package rabbitmq

import (
	pcontract "github.com/prismgo/framework/contracts/provider"
	qcontract "github.com/prismgo/framework/contracts/queue"
	"github.com/prismgo/framework/queue"
)

// ServiceProvider installs RabbitMQ on an application's queue manager.
type ServiceProvider struct{}

// Name returns the provider's stable lifecycle identity.
func (ServiceProvider) Name() string { return "prismgo.extension.rabbitmq" }

// Register declares no eager bindings because RabbitMQ is installed during Boot.
func (ServiceProvider) Register(pcontract.Application) error { return nil }

// Boot registers the RabbitMQ connector without opening a broker connection.
func (ServiceProvider) Boot(app pcontract.Application) error {
	manager, err := queue.ManagerFrom(app.Container())
	if err != nil {
		return err
	}
	manager.Extend("rabbitmq", func() (qcontract.Connector, error) {
		return Connector{}, nil
	})
	return nil
}

var _ pcontract.ServiceProvider = ServiceProvider{}
var _ pcontract.NamedProvider = ServiceProvider{}
