package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const horizonIntegrationRabbitMQEnv = "PRISMGO_RABBITMQ_TEST_URL"
const horizonIntegrationRabbitMQNameEnv = "PRISMGO_RABBITMQ_HORIZON_TEST_NAME"

func TestRabbitMQHorizonIntegrationGateSkipsUnsupportedFailedAndBatchState(t *testing.T) {
	if strings.TrimSpace(os.Getenv(horizonIntegrationRabbitMQEnv)) == "" {
		t.Skipf("%s is not set; skipping RabbitMQ Horizon integration gate", horizonIntegrationRabbitMQEnv)
	}
	ctx, cancel := horizonIntegrationContext(t)
	defer cancel()

	if output, err := exec.CommandContext(ctx, "go", "list", "-m", "github.com/prismgo/horizon").CombinedOutput(); err != nil {
		t.Skipf("github.com/prismgo/horizon is not installed; skipping RabbitMQ Horizon integration gate: %s", strings.TrimSpace(string(output)))
	}
	fixture := newRabbitMQIntegrationFixture(t, "horizon_integration")

	fixturePath := filepath.Join(t.TempDir(), "horizon-integration.test")
	if runtime.GOOS == "windows" {
		fixturePath += ".exe"
	}
	compile := exec.CommandContext(
		ctx,
		"go", "test", "-c", "-o", fixturePath, "./testdata/horizon_integration",
	)
	compile.Env = os.Environ()
	output, err := compile.CombinedOutput()
	if err != nil {
		t.Fatalf("compile installed Horizon integration fixture: %v\n%s", err, output)
	}

	run := exec.CommandContext(
		ctx,
		fixturePath,
		"-test.run", "^TestRabbitMQHorizonIntegration$",
		"-test.count=1",
		"-test.v",
		"-test.timeout=2m",
	)
	run.Env = append(os.Environ(), horizonIntegrationRabbitMQNameEnv+"="+fixture.name)
	output, err = run.CombinedOutput()
	if err != nil {
		t.Fatalf("run installed Horizon integration fixture: %v\n%s", err, output)
	}
	t.Logf("installed Horizon integration fixture passed:\n%s", output)
}

func horizonIntegrationContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	if deadline, ok := t.Deadline(); ok {
		deadline = deadline.Add(-5 * time.Second)
		if !deadline.After(time.Now()) {
			t.Fatal("RabbitMQ Horizon integration gate has no time remaining before the test deadline")
		}

		return context.WithDeadline(t.Context(), deadline)
	}

	return context.WithTimeout(t.Context(), 2*time.Minute)
}
