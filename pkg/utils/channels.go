package utils

import (
	"fmt"
	"strings"

	"github.com/riven-blade/datahive/pkg/protocol"
	"github.com/spf13/cast"
)

// GenerateTopic 生成 topic（也是订阅 ID）
// 格式: {exchange}_{market}_{symbol}_{eventType}[_{params}]
func GenerateTopic(exchange, market, symbol, eventType, interval string, depth int) string {
	symbol = strings.ToLower(symbol)
	switch strings.ToLower(eventType) {
	case protocol.StreamEventKline:
		if interval == "" {
			interval = "1m" // 默认1分钟
		}
		return fmt.Sprintf("%s_%s_%s_%s_%s", exchange, market, symbol, eventType, interval)
	case protocol.StreamEventOrderBook:
		if depth > 0 {
			return fmt.Sprintf("%s_%s_%s_%s_%d", exchange, market, symbol, eventType, depth)
		}
		return fmt.Sprintf("%s_%s_%s_%s", exchange, market, symbol, eventType)
	case protocol.StreamEventTrade, protocol.StreamEventMiniTicker, protocol.StreamEventBookTicker, protocol.StreamEventMarkPrice:
		return fmt.Sprintf("%s_%s_%s_%s", exchange, market, symbol, eventType)
	default:
		return fmt.Sprintf("%s_%s_%s_%s", exchange, market, symbol, eventType)
	}
}

// SafeGetString 安全获取 map 中的字符串值
func SafeGetString(m map[string]interface{}, key string) string {
	return cast.ToString(m[key])
}

// SafeGetInt 安全获取 map 中的整数值
func SafeGetInt(m map[string]interface{}, key string) int {
	return cast.ToInt(m[key])
}

// SafeGetBool 安全获取 map 中的布尔值
func SafeGetBool(m map[string]interface{}, key string) bool {
	return cast.ToBool(m[key])
}

// SafeGetFloat 安全获取 map 中的浮点数值
func SafeGetFloat(m map[string]interface{}, key string) float64 {
	return cast.ToFloat64(m[key])
}

// SafeGetStringWithDefault 安全获取 map 中的字符串值，带默认值
func SafeGetStringWithDefault(m map[string]interface{}, key, defaultValue string) string {
	if value := cast.ToString(m[key]); value != "" {
		return value
	}
	return defaultValue
}

// SafeGetIntWithDefault 安全获取 map 中的整数值，带默认值
func SafeGetIntWithDefault(m map[string]interface{}, key string, defaultValue int) int {
	if value := cast.ToInt(m[key]); value != 0 {
		return value
	}
	return defaultValue
}

// SafeGetFloatWithDefault 安全获取 map 中的浮点数值，带默认值
func SafeGetFloatWithDefault(m map[string]interface{}, key string, defaultValue float64) float64 {
	if value := cast.ToFloat64(m[key]); value != 0 {
		return value
	}
	return defaultValue
}

// SafeGetInt64WithDefault 安全获取 map 中的 int64 值，带默认值
func SafeGetInt64WithDefault(m map[string]interface{}, key string, defaultValue int64) int64 {
	if value := cast.ToInt64(m[key]); value != 0 {
		return value
	}
	return defaultValue
}

// SafeGetBoolWithDefault 安全获取 map 中的布尔值，带默认值
func SafeGetBoolWithDefault(m map[string]interface{}, key string, defaultValue bool) bool {
	if value, exists := m[key]; exists {
		return cast.ToBool(value)
	}
	return defaultValue
}
