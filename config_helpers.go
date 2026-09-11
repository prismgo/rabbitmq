package rabbitmq

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func secondsValue(value any, fallback int) time.Duration {
	return time.Duration(castInt(value, fallback)) * time.Second
}

func castInt(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return fallback
		}
		return typed
	case int64:
		if typed < 0 {
			return fallback
		}
		return int(typed)
	case float64:
		if typed < 0 {
			return fallback
		}
		return int(typed)
	case string:
		return parsePositiveInt(typed, fallback)
	default:
		return parsePositiveInt(castString(value), fallback)
	}
}

// castString 将任意类型转换为字符串。
//
// 支持类型：string、[]byte、fmt.Stringer（如 net.IP、time.Duration 等）。
// 无法识别的类型返回空字符串，避免 panic。
func castString(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	}
	return ""
}

func castBool(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	}
	return fallback
}

func parsePositiveInt(value string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
