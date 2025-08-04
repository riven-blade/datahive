package ccxt

import (
	"fmt"
	"strings"
	"sync"
)

// 全局交易所注册表
var (
	exchangeRegistry = make(map[string]ExchangeCreator)
	registryMutex    sync.RWMutex
)

// RegisterExchange 注册交易所
// 每个交易所包在init()函数中调用此函数注册自己
func RegisterExchange(name string, creator ExchangeCreator) {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	exchangeRegistry[strings.ToLower(name)] = creator
}

// CreateExchange 根据配置创建交易所实例
func CreateExchange(name string, config ExchangeConfig) (Exchange, error) {
	registryMutex.RLock()
	creator, exists := exchangeRegistry[strings.ToLower(name)]
	registryMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported exchange: %s", name)
	}

	return creator(config)
}

// GetSupportedExchanges 获取所有支持的交易所列表
func GetSupportedExchanges() []string {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	exchanges := make([]string, 0, len(exchangeRegistry))
	for name := range exchangeRegistry {
		exchanges = append(exchanges, name)
	}

	return exchanges
}

// IsExchangeSupported 检查交易所是否支持
func IsExchangeSupported(name string) bool {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	_, exists := exchangeRegistry[strings.ToLower(name)]
	return exists
}
