package rabbitmq

import (
	"testing"
	"time"
)

func TestConfigCastHelpersCoverSupportedShapes(t *testing.T) {
	fallback := 9 * time.Second
	if got := castInt(int64(-1), 7); got != 7 {
		t.Fatalf("castInt(negative int64) = %d, want 7", got)
	}
	if got := castInt(float64(-1), 7); got != 7 {
		t.Fatalf("castInt(negative float64) = %d, want 7", got)
	}
	if got := castInt([]byte("5"), 7); got != 5 {
		t.Fatalf("castInt(byte slice) = %d, want 5", got)
	}
	if got := castInt(nil, 7); got != 7 {
		t.Fatalf("castInt(nil) = %d, want 7", got)
	}
	// RabbitMQ 配置支持秒数、Go duration 字符串和测试直接传入的强类型 duration。
	durationCases := []struct {
		name  string
		value any
		want  time.Duration
	}{
		{name: "duration", value: 1500 * time.Millisecond, want: 1500 * time.Millisecond},
		{name: "int seconds", value: 2, want: 2 * time.Second},
		{name: "int64 seconds", value: int64(3), want: 3 * time.Second},
		{name: "float seconds", value: 1.5, want: 1500 * time.Millisecond},
		{name: "string duration", value: "250ms", want: 250 * time.Millisecond},
		{name: "string seconds", value: "4", want: 4 * time.Second},
		{name: "empty", value: "", want: fallback},
		{name: "negative", value: -1, want: fallback},
	}
	for _, tc := range durationCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := castDurationValue(tc.value, fallback); got != tc.want {
				t.Fatalf("duration = %v, want %v", got, tc.want)
			}
		})
	}

	if got := castDurationBuckets([]time.Duration{time.Second, 2 * time.Second}, nil); len(got) != 2 || got[1] != 2*time.Second {
		t.Fatalf("duration bucket slice = %v", got)
	}
	if got := castDurationBuckets([]int{1, 0, 3}, nil); len(got) != 2 || got[1] != 3*time.Second {
		t.Fatalf("int bucket slice = %v", got)
	}
	if got := castDurationBuckets([]any{"2", int64(4), -1}, nil); len(got) != 2 || got[1] != 4*time.Second {
		t.Fatalf("any bucket slice = %v", got)
	}
	if got := castDurationBuckets("5, bad, 7", nil); len(got) != 2 || got[1] != 7*time.Second {
		t.Fatalf("string bucket slice = %v", got)
	}
	if got := castDurationBuckets(-1, []time.Duration{fallback}); len(got) != 1 || got[0] != fallback {
		t.Fatalf("fallback bucket slice = %v", got)
	}

	boolCases := []struct {
		value    any
		fallback bool
		want     bool
	}{
		{value: true, fallback: false, want: true},
		{value: "yes", fallback: false, want: true},
		{value: "off", fallback: true, want: false},
		{value: 1, fallback: false, want: true},
		{value: int64(0), fallback: true, want: false},
		{value: 2.0, fallback: false, want: true},
		{value: "unknown", fallback: true, want: true},
	}
	for _, tc := range boolCases {
		if got := castBool(tc.value, tc.fallback); got != tc.want {
			t.Fatalf("castBool(%v) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestRabbitMQConfigParsingBranches(t *testing.T) {
	spec := map[string]any{
		"url":                        "amqp://guest:secret@127.0.0.1:5672/",
		"scheme":                     "amqps",
		"host":                       "rabbit.local",
		"port":                       5671,
		"username":                   "guest",
		"password":                   "secret",
		"vhost":                      "tenant",
		"exchange":                   "jobs",
		"exchange_type":              "topic",
		"declare":                    "true",
		"exchange_durable":           false,
		"queue_durable":              "false",
		"queue_max_priority":         "9",
		"message_persistent":         "false",
		"auto_delete":                true,
		"exclusive":                  "true",
		"no_wait":                    false,
		"confirm":                    "true",
		"delay_mode":                 "ttl_dlx",
		"delay_buckets":              "1, 5s, bad",
		"prefetch":                   "3",
		"heartbeat":                  "2s",
		"publish_timeout":            4,
		"publish_channels":           "2",
		"reconnect_min_delay":        "100ms",
		"reconnect_max_delay":        "2s",
		"restart_queue":              "restart",
		"restart_enabled":            "false",
		"restart_poll_interval":      "250ms",
		"topology_cache_ttl":         "30s",
		"topology_cache_max_entries": "20",
	}
	opts := rabbitMQOptionsFromMap(spec)
	if opts.Exchange != "jobs" || opts.ExchangeType != "topic" || opts.Prefetch != 3 || opts.PublishChannels != 2 {
		t.Fatalf("rabbit options = %#v", opts)
	}
	if opts.QueueMaxPriority != 9 || opts.DelayMode != "ttl_dlx" || len(opts.DelayBuckets) != 1 {
		t.Fatalf("rabbit numeric options = %#v", opts)
	}
	if opts.RestartEnabled.Or(true) || opts.RestartPollInterval != 250*time.Millisecond || opts.TopologyCacheTTL != 30*time.Second {
		t.Fatalf("rabbit restart/cache options = %#v", opts)
	}
	defaulted := rabbitMQOptionsFromMap(map[string]any{"url": "amqp://guest:guest@example.test:5672/%2F"})
	if defaulted.Confirm.IsSet() || defaulted.Declare.IsSet() || defaulted.RestartEnabled.IsSet() {
		t.Fatalf("missing rabbit bool keys = %#v, want unset bools for runtime defaults", defaulted)
	}
	disabledConfirm := rabbitMQOptionsFromMap(map[string]any{
		"url":     "amqp://guest:guest@example.test:5672/%2F",
		"confirm": false,
	})
	if !disabledConfirm.Confirm.IsSet() || disabledConfirm.Confirm.Or(true) {
		t.Fatalf("explicit confirm=false = %#v, want explicit false retained", disabledConfirm)
	}
}
