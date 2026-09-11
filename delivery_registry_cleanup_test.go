package rabbitmq

import (
	"testing"
)

// TestDeliveryRegistryCleanup_CanBeStopped 验证清理 goroutine 可以从外部停止。
func TestDeliveryRegistryCleanup_CanBeStopped(t *testing.T) {
	stopDeliveryRegistryCleanup()

	// 启动清理 goroutine
	startDeliveryRegistryCleanup()

	// 验证已启动
	if !deliveryRegistryCleanupRunning() {
		t.Fatal("expected cleanup to be started")
	}

	// 停止清理 goroutine
	stopDeliveryRegistryCleanup()

	if deliveryRegistryCleanupRunning() {
		t.Fatal("expected cleanup to be stopped")
	}

	// 再次启动应该可以成功
	startDeliveryRegistryCleanup()

	if !deliveryRegistryCleanupRunning() {
		t.Fatal("expected cleanup to be restarted")
	}

	// 清理
	stopDeliveryRegistryCleanup()
}
