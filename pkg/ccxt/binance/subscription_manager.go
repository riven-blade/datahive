package binance

import (
	"fmt"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"

	"go.uber.org/zap"
)

// SubscriptionManager 订阅管理器 - 负责将数据分发到正确的订阅者
type SubscriptionManager struct {
	mu sync.RWMutex

	// 按数据类型和symbol分组的订阅者
	miniTickerSubscribers map[string][]chan *ccxt.WatchMiniTicker // key: symbol
	orderBookSubscribers  map[string][]chan *ccxt.WatchOrderBook  // key: symbol
	tradeSubscribers      map[string][]chan *ccxt.WatchTrade      // key: symbol
	ohlcvSubscribers      map[string][]chan *ccxt.WatchOHLCV      // key: symbol_timeframe
	balanceSubscribers    []chan *ccxt.WatchBalance               // 余额不需要symbol分组
	orderSubscribers      []chan *ccxt.WatchOrder                 // 订单不需要symbol分组

	// 订阅计数器 - 跟踪每个symbol的订阅数量
	subscriptionCount map[string]int // key: symbol_datatype

	// 订阅ID映射 - 用于取消订阅
	subscriptionChannels map[string]interface{} // key: subscriptionID, value: channel
}

// NewSubscriptionManager 创建订阅管理器
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		miniTickerSubscribers: make(map[string][]chan *ccxt.WatchMiniTicker),
		orderBookSubscribers:  make(map[string][]chan *ccxt.WatchOrderBook),
		tradeSubscribers:      make(map[string][]chan *ccxt.WatchTrade),
		ohlcvSubscribers:      make(map[string][]chan *ccxt.WatchOHLCV),
		balanceSubscribers:    make([]chan *ccxt.WatchBalance, 0),
		orderSubscribers:      make([]chan *ccxt.WatchOrder, 0),
		subscriptionCount:     make(map[string]int),
		subscriptionChannels:  make(map[string]interface{}),
	}
}

// SubscribeMiniTicker 订阅迷你ticker数据
func (sm *SubscriptionManager) SubscribeMiniTicker(symbol string) (string, <-chan *ccxt.WatchMiniTicker) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 创建专用channel
	userChan := make(chan *ccxt.WatchMiniTicker, 1000)

	// 生成唯一订阅ID
	subscriptionID := fmt.Sprintf("miniticker_%s_%d", symbol, time.Now().UnixNano())

	// 注册订阅
	if sm.miniTickerSubscribers[symbol] == nil {
		sm.miniTickerSubscribers[symbol] = make([]chan *ccxt.WatchMiniTicker, 0)
	}
	sm.miniTickerSubscribers[symbol] = append(sm.miniTickerSubscribers[symbol], userChan)

	// 记录订阅ID映射
	sm.subscriptionChannels[subscriptionID] = userChan

	// 更新计数
	countKey := fmt.Sprintf("%s_miniticker", symbol)
	sm.subscriptionCount[countKey]++

	logger.Debug("Subscribed to mini ticker",
		zap.String("symbol", symbol),
		zap.String("subscription_id", subscriptionID),
		zap.Int("subscriber_count", len(sm.miniTickerSubscribers[symbol])))

	return subscriptionID, userChan
}

// SubscribeOrderBook 订阅订单簿数据
func (sm *SubscriptionManager) SubscribeOrderBook(symbol string) (string, <-chan *ccxt.WatchOrderBook) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	userChan := make(chan *ccxt.WatchOrderBook, 500)
	subscriptionID := fmt.Sprintf("orderbook_%s_%d", symbol, time.Now().UnixNano())

	if sm.orderBookSubscribers[symbol] == nil {
		sm.orderBookSubscribers[symbol] = make([]chan *ccxt.WatchOrderBook, 0)
	}
	sm.orderBookSubscribers[symbol] = append(sm.orderBookSubscribers[symbol], userChan)

	sm.subscriptionChannels[subscriptionID] = userChan

	countKey := fmt.Sprintf("%s_orderbook", symbol)
	sm.subscriptionCount[countKey]++

	return subscriptionID, userChan
}

// SubscribeTrades 订阅交易数据
func (sm *SubscriptionManager) SubscribeTrades(symbol string) (string, <-chan *ccxt.WatchTrade) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	userChan := make(chan *ccxt.WatchTrade, 1000)
	subscriptionID := fmt.Sprintf("trades_%s_%d", symbol, time.Now().UnixNano())

	if sm.tradeSubscribers[symbol] == nil {
		sm.tradeSubscribers[symbol] = make([]chan *ccxt.WatchTrade, 0)
	}
	sm.tradeSubscribers[symbol] = append(sm.tradeSubscribers[symbol], userChan)

	sm.subscriptionChannels[subscriptionID] = userChan

	countKey := fmt.Sprintf("%s_trades", symbol)
	sm.subscriptionCount[countKey]++

	return subscriptionID, userChan
}

// SubscribeOHLCV 订阅K线数据
func (sm *SubscriptionManager) SubscribeOHLCV(symbol, timeframe string) (string, <-chan *ccxt.WatchOHLCV) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	userChan := make(chan *ccxt.WatchOHLCV, 500)
	subscriptionID := fmt.Sprintf("ohlcv_%s_%s_%d", symbol, timeframe, time.Now().UnixNano())

	key := fmt.Sprintf("%s_%s", symbol, timeframe)
	if sm.ohlcvSubscribers[key] == nil {
		sm.ohlcvSubscribers[key] = make([]chan *ccxt.WatchOHLCV, 0)
	}
	sm.ohlcvSubscribers[key] = append(sm.ohlcvSubscribers[key], userChan)

	sm.subscriptionChannels[subscriptionID] = userChan

	countKey := fmt.Sprintf("%s_%s_ohlcv", symbol, timeframe)
	sm.subscriptionCount[countKey]++

	return subscriptionID, userChan
}

// SubscribeBalance 订阅余额数据
func (sm *SubscriptionManager) SubscribeBalance() (string, <-chan *ccxt.WatchBalance) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	userChan := make(chan *ccxt.WatchBalance, 100)
	subscriptionID := fmt.Sprintf("balance_%d", time.Now().UnixNano())

	sm.balanceSubscribers = append(sm.balanceSubscribers, userChan)
	sm.subscriptionChannels[subscriptionID] = userChan

	sm.subscriptionCount["balance"]++

	return subscriptionID, userChan
}

// SubscribeOrders 订阅订单数据
func (sm *SubscriptionManager) SubscribeOrders() (string, <-chan *ccxt.WatchOrder) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	userChan := make(chan *ccxt.WatchOrder, 100)
	subscriptionID := fmt.Sprintf("orders_%d", time.Now().UnixNano())

	sm.orderSubscribers = append(sm.orderSubscribers, userChan)
	sm.subscriptionChannels[subscriptionID] = userChan

	sm.subscriptionCount["orders"]++

	return subscriptionID, userChan
}

// DistributeMiniTicker 分发mini ticker数据到所有订阅者
func (sm *SubscriptionManager) DistributeMiniTicker(symbol string, data *ccxt.WatchMiniTicker) {
	sm.mu.RLock()
	subscribers := sm.miniTickerSubscribers[symbol]
	sm.mu.RUnlock()

	if len(subscribers) == 0 {
		return
	}

	// 非阻塞分发
	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
			// channel满了，跳过这个订阅者
		}
	}
}

// DistributeOrderBook 分发订单簿数据
func (sm *SubscriptionManager) DistributeOrderBook(symbol string, data *ccxt.WatchOrderBook) {
	sm.mu.RLock()
	subscribers := sm.orderBookSubscribers[symbol]
	sm.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// DistributeTrades 分发交易数据
func (sm *SubscriptionManager) DistributeTrades(symbol string, data *ccxt.WatchTrade) {
	sm.mu.RLock()
	subscribers := sm.tradeSubscribers[symbol]
	sm.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// DistributeOHLCV 分发K线数据
func (sm *SubscriptionManager) DistributeOHLCV(symbol, timeframe string, data *ccxt.WatchOHLCV) {
	key := fmt.Sprintf("%s_%s", symbol, timeframe)

	sm.mu.RLock()
	subscribers := sm.ohlcvSubscribers[key]
	sm.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// DistributeBalance 分发余额数据
func (sm *SubscriptionManager) DistributeBalance(data *ccxt.WatchBalance) {
	sm.mu.RLock()
	subscribers := sm.balanceSubscribers
	sm.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// DistributeOrders 分发订单数据
func (sm *SubscriptionManager) DistributeOrders(data *ccxt.WatchOrder) {
	sm.mu.RLock()
	subscribers := sm.orderSubscribers
	sm.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- data:
		default:
		}
	}
}

// IsFirstSubscription 检查是否是某个symbol+datatype的第一个订阅
func (sm *SubscriptionManager) IsFirstSubscription(symbol, dataType string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	countKey := fmt.Sprintf("%s_%s", symbol, dataType)
	return sm.subscriptionCount[countKey] == 1
}

// Unsubscribe 取消订阅
func (sm *SubscriptionManager) Unsubscribe(subscriptionID string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ch, exists := sm.subscriptionChannels[subscriptionID]
	if !exists {
		return fmt.Errorf("subscription not found: %s", subscriptionID)
	}

	// 根据订阅ID前缀确定类型并移除
	switch ch := ch.(type) {
	case chan *ccxt.WatchMiniTicker:
		sm.removeMiniTickerSubscription(ch)
	case chan *ccxt.WatchOrderBook:
		sm.removeOrderBookSubscription(ch)
	case chan *ccxt.WatchTrade:
		sm.removeTradeSubscription(ch)
	case chan *ccxt.WatchOHLCV:
		sm.removeOHLCVSubscription(ch)
	case chan *ccxt.WatchBalance:
		sm.removeBalanceSubscription(ch)
	case chan *ccxt.WatchOrder:
		sm.removeOrderSubscription(ch)
	}

	// 关闭channel
	switch ch := ch.(type) {
	case chan *ccxt.WatchMiniTicker:
		close(ch)
	case chan *ccxt.WatchOrderBook:
		close(ch)
	case chan *ccxt.WatchTrade:
		close(ch)
	case chan *ccxt.WatchOHLCV:
		close(ch)
	case chan *ccxt.WatchBalance:
		close(ch)
	case chan *ccxt.WatchOrder:
		close(ch)
	}

	delete(sm.subscriptionChannels, subscriptionID)

	return nil
}

// removeMiniTickerSubscription 移除mini ticker订阅（辅助方法）
func (sm *SubscriptionManager) removeMiniTickerSubscription(targetCh chan *ccxt.WatchMiniTicker) {
	for symbol, subscribers := range sm.miniTickerSubscribers {
		for i, ch := range subscribers {
			if ch == targetCh {
				// 移除该订阅者
				sm.miniTickerSubscribers[symbol] = append(subscribers[:i], subscribers[i+1:]...)

				// 更新计数
				countKey := fmt.Sprintf("%s_miniticker", symbol)
				sm.subscriptionCount[countKey]--

				if sm.subscriptionCount[countKey] == 0 {
					delete(sm.subscriptionCount, countKey)
					delete(sm.miniTickerSubscribers, symbol)
				}
				return
			}
		}
	}
}

// 类似的移除方法为其他数据类型实现...
func (sm *SubscriptionManager) removeOrderBookSubscription(targetCh chan *ccxt.WatchOrderBook) {
	// 实现类似的逻辑
}

func (sm *SubscriptionManager) removeTradeSubscription(targetCh chan *ccxt.WatchTrade) {
	// 实现类似的逻辑
}

func (sm *SubscriptionManager) removeOHLCVSubscription(targetCh chan *ccxt.WatchOHLCV) {
	// 实现类似的逻辑
}

func (sm *SubscriptionManager) removeBalanceSubscription(targetCh chan *ccxt.WatchBalance) {
	// 实现类似的逻辑
}

func (sm *SubscriptionManager) removeOrderSubscription(targetCh chan *ccxt.WatchOrder) {
	// 实现类似的逻辑
}

// GetStats 获取订阅统计信息
func (sm *SubscriptionManager) GetStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return map[string]interface{}{
		"miniticker_symbols":  len(sm.miniTickerSubscribers),
		"orderbook_symbols":   len(sm.orderBookSubscribers),
		"trades_symbols":      len(sm.tradeSubscribers),
		"ohlcv_symbols":       len(sm.ohlcvSubscribers),
		"balance_subscribers": len(sm.balanceSubscribers),
		"order_subscribers":   len(sm.orderSubscribers),
		"total_subscriptions": len(sm.subscriptionChannels),
		"subscription_counts": sm.subscriptionCount,
	}
}

// Close 关闭所有订阅
func (sm *SubscriptionManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 关闭所有channels
	for _, ch := range sm.subscriptionChannels {
		switch ch := ch.(type) {
		case chan *ccxt.WatchMiniTicker:
			close(ch)
		case chan *ccxt.WatchOrderBook:
			close(ch)
		case chan *ccxt.WatchTrade:
			close(ch)
		case chan *ccxt.WatchOHLCV:
			close(ch)
		case chan *ccxt.WatchBalance:
			close(ch)
		case chan *ccxt.WatchOrder:
			close(ch)
		}
	}

	// 清理所有映射
	sm.miniTickerSubscribers = make(map[string][]chan *ccxt.WatchMiniTicker)
	sm.orderBookSubscribers = make(map[string][]chan *ccxt.WatchOrderBook)
	sm.tradeSubscribers = make(map[string][]chan *ccxt.WatchTrade)
	sm.ohlcvSubscribers = make(map[string][]chan *ccxt.WatchOHLCV)
	sm.balanceSubscribers = make([]chan *ccxt.WatchBalance, 0)
	sm.orderSubscribers = make([]chan *ccxt.WatchOrder, 0)
	sm.subscriptionCount = make(map[string]int)
	sm.subscriptionChannels = make(map[string]interface{})
}
