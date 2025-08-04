package binance

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"

	"go.uber.org/zap"
)

const (
	// 安全阈值 - 当接近限制时创建新实例
	maxStreamsPerInstance = 1000 // 留24个安全余量
	maxInstancesAllowed   = 20   // 最大实例数量限制
)

// WebSocketPool WebSocket连接池管理器
type WebSocketPool struct {
	mu       sync.RWMutex
	exchange *Binance

	// WebSocket实例管理
	instances []*PooledWebSocket

	// symbol到实例的映射 - 确保同一symbol总是使用同一实例
	symbolToInstance map[string]*PooledWebSocket

	// 状态
	running    bool
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// PooledWebSocket 池化的WebSocket实例
type PooledWebSocket struct {
	*BinanceWebSocket
	id             int
	connID         string    // 连接ID
	subscriptions  int32     // 原子操作的订阅计数
	reconnectCount int32     // 重连次数
	lastHealthy    time.Time // 最后健康时间
	createdAt      time.Time
}

// NewWebSocketPool 创建WebSocket连接池
func NewWebSocketPool(exchange *Binance) *WebSocketPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WebSocketPool{
		exchange:         exchange,
		ctx:              ctx,
		cancelFunc:       cancel,
		symbolToInstance: make(map[string]*PooledWebSocket),
		running:          true,
	}

	// 启动连接池监控
	go pool.startPoolHealthMonitor(ctx)

	// 创建第一个WebSocket实例
	if err := pool.createInstance(); err != nil {
		logger.Error("Failed to create initial WebSocket instance", zap.Error(err))
	}

	return pool
}

// createInstance 创建新的WebSocket实例
func (p *WebSocketPool) createInstance() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查实例数量限制
	if len(p.instances) >= maxInstancesAllowed {
		return fmt.Errorf("reached maximum instances limit: %d", maxInstancesAllowed)
	}

	instanceID := len(p.instances)
	connID := fmt.Sprintf("pool-%d-ws-%d", time.Now().UnixNano(), instanceID)

	// 创建WebSocket实例
	ws := NewBinanceWebSocket(p.exchange)

	pooled := &PooledWebSocket{
		BinanceWebSocket: ws,
		id:               instanceID,
		connID:           connID,
		subscriptions:    0,
		reconnectCount:   0,
		lastHealthy:      time.Now(),
		createdAt:        time.Now(),
	}

	p.instances = append(p.instances, pooled)

	logger.Info("Created new WebSocket instance",
		zap.Int("instance_id", instanceID),
		zap.String("conn_id", connID),
		zap.Int("total_instances", len(p.instances)))

	return nil
}

// getInstanceForSymbol 为symbol获取或分配实例
func (p *WebSocketPool) getInstanceForSymbol(symbol string) (*PooledWebSocket, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查是否已经有实例处理该symbol
	if instance, exists := p.symbolToInstance[symbol]; exists {
		// 检查实例是否健康
		if p.isInstanceHealthy(instance) {
			return instance, nil
		}
		// 实例不健康，需要重新分配
		delete(p.symbolToInstance, symbol)
	}

	// 寻找健康且有空间的实例
	for _, instance := range p.instances {
		if p.isInstanceHealthy(instance) &&
			atomic.LoadInt32(&instance.subscriptions) < maxStreamsPerInstance {
			p.symbolToInstance[symbol] = instance
			return instance, nil
		}
	}

	// 所有实例都满了或不健康，尝试创建新实例
	if len(p.instances) >= maxInstancesAllowed {
		return nil, fmt.Errorf("reached maximum instances limit and all instances are full")
	}

	// 解锁后创建实例，避免死锁
	p.mu.Unlock()
	err := p.createInstance()
	p.mu.Lock()

	if err != nil {
		return nil, fmt.Errorf("failed to create new instance: %w", err)
	}

	// 返回新创建的实例
	if len(p.instances) > 0 {
		newInstance := p.instances[len(p.instances)-1]
		p.symbolToInstance[symbol] = newInstance

		logger.Info("Created new WebSocket instance for symbol",
			zap.String("symbol", symbol),
			zap.Int("instance_id", newInstance.id),
			zap.String("conn_id", newInstance.connID),
			zap.Int("total_instances", len(p.instances)))

		return newInstance, nil
	}

	return nil, fmt.Errorf("failed to create instance")
}

// WatchPrice 订阅价格数据 - 返回订阅ID和专用频道
func (p *WebSocketPool) WatchPrice(ctx context.Context, symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchPrice, error) {
	instance, err := p.getInstanceForSymbol(symbol)
	if err != nil {
		return "", nil, err
	}

	// 连接实例（如果尚未连接）
	if !instance.IsConnected() {
		if err := instance.Connect(ctx); err != nil {
			return "", nil, fmt.Errorf("failed to connect instance %d: %w", instance.id, err)
		}
	}

	// 调用实例的WatchPrice
	subscriptionID, resultChan, err := instance.WatchPrice(ctx, symbol, params)
	if err != nil {
		return "", nil, err
	}

	// 增加订阅计数
	atomic.AddInt32(&instance.subscriptions, 1)

	// 返回订阅ID和专用频道
	return subscriptionID, resultChan, nil
}

// WatchOHLCV 订阅K线数据 - 返回订阅ID和专用频道
func (p *WebSocketPool) WatchOHLCV(ctx context.Context, symbol, timeframe string, params map[string]interface{}) (string, <-chan *ccxt.WatchOHLCV, error) {
	instance, err := p.getInstanceForSymbol(symbol)
	if err != nil {
		return "", nil, err
	}

	if !instance.IsConnected() {
		if err := instance.Connect(ctx); err != nil {
			return "", nil, fmt.Errorf("failed to connect instance %d: %w", instance.id, err)
		}
	}

	subscriptionID, resultChan, err := instance.WatchOHLCV(ctx, symbol, timeframe, params)
	if err != nil {
		return "", nil, err
	}

	atomic.AddInt32(&instance.subscriptions, 1)
	return subscriptionID, resultChan, nil
}

// WatchTrades 订阅交易数据 - 返回订阅ID和专用频道
func (p *WebSocketPool) WatchTrades(ctx context.Context, symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchTrade, error) {
	instance, err := p.getInstanceForSymbol(symbol)
	if err != nil {
		return "", nil, err
	}

	if !instance.IsConnected() {
		if err := instance.Connect(ctx); err != nil {
			return "", nil, fmt.Errorf("failed to connect instance %d: %w", instance.id, err)
		}
	}

	subscriptionID, resultChan, err := instance.WatchTrades(ctx, symbol, params)
	if err != nil {
		return "", nil, err
	}

	atomic.AddInt32(&instance.subscriptions, 1)
	return subscriptionID, resultChan, nil
}

// WatchOrderBook 订阅订单簿数据 - 返回订阅ID和专用频道
func (p *WebSocketPool) WatchOrderBook(ctx context.Context, symbol string, params map[string]interface{}) (string, <-chan *ccxt.WatchOrderBook, error) {
	instance, err := p.getInstanceForSymbol(symbol)
	if err != nil {
		return "", nil, err
	}

	if !instance.IsConnected() {
		if err := instance.Connect(ctx); err != nil {
			return "", nil, fmt.Errorf("failed to connect instance %d: %w", instance.id, err)
		}
	}

	subscriptionID, resultChan, err := instance.WatchOrderBook(ctx, symbol, params)
	if err != nil {
		return "", nil, err
	}

	atomic.AddInt32(&instance.subscriptions, 1)
	return subscriptionID, resultChan, nil
}

// WatchBalance 订阅余额数据 - 返回订阅ID和专用频道
func (p *WebSocketPool) WatchBalance(ctx context.Context, params map[string]interface{}) (string, <-chan *ccxt.WatchBalance, error) {
	// 余额数据不绑定symbol，使用第一个可用实例
	instance, err := p.getFirstAvailableInstance()
	if err != nil {
		return "", nil, err
	}

	if !instance.IsConnected() {
		if err := instance.Connect(ctx); err != nil {
			return "", nil, fmt.Errorf("failed to connect instance %d: %w", instance.id, err)
		}
	}

	subscriptionID, resultChan, err := instance.WatchBalance(ctx, params)
	if err != nil {
		return "", nil, err
	}

	atomic.AddInt32(&instance.subscriptions, 1)
	return subscriptionID, resultChan, nil
}

// WatchOrders 订阅订单数据 - 返回订阅ID和专用频道
func (p *WebSocketPool) WatchOrders(ctx context.Context, params map[string]interface{}) (string, <-chan *ccxt.WatchOrder, error) {
	// 订单数据不绑定symbol，使用第一个可用实例
	instance, err := p.getFirstAvailableInstance()
	if err != nil {
		return "", nil, err
	}

	if !instance.IsConnected() {
		if err := instance.Connect(ctx); err != nil {
			return "", nil, fmt.Errorf("failed to connect instance %d: %w", instance.id, err)
		}
	}

	subscriptionID, resultChan, err := instance.WatchOrders(ctx, params)
	if err != nil {
		return "", nil, err
	}

	atomic.AddInt32(&instance.subscriptions, 1)
	return subscriptionID, resultChan, nil
}

// getFirstAvailableInstance 获取第一个可用实例
func (p *WebSocketPool) getFirstAvailableInstance() (*PooledWebSocket, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, instance := range p.instances {
		if atomic.LoadInt32(&instance.subscriptions) < maxStreamsPerInstance {
			return instance, nil
		}
	}

	// 没有可用实例，需要创建新的
	p.mu.RUnlock()
	p.mu.Lock()
	defer func() {
		p.mu.Unlock()
		p.mu.RLock()
	}()

	if len(p.instances) >= maxInstancesAllowed {
		return nil, fmt.Errorf("reached maximum instances limit and all instances are full")
	}

	// 创建新实例
	instanceID := len(p.instances)
	connID := fmt.Sprintf("pool-%d-ws-%d", time.Now().UnixNano(), instanceID)
	ws := NewBinanceWebSocket(p.exchange)

	pooled := &PooledWebSocket{
		BinanceWebSocket: ws,
		id:               instanceID,
		connID:           connID,
		subscriptions:    0,
		reconnectCount:   0,
		lastHealthy:      time.Now(),
		createdAt:        time.Now(),
	}

	p.instances = append(p.instances, pooled)

	logger.Info("Created new WebSocket instance for balance/orders",
		zap.Int("instance_id", instanceID),
		zap.Int("total_instances", len(p.instances)))

	return pooled, nil
}

// ========== 健康检查和监控 ==========

// isInstanceHealthy 检查实例是否健康
func (p *WebSocketPool) isInstanceHealthy(instance *PooledWebSocket) bool {
	if instance == nil {
		return false
	}

	// 检查WebSocket连接状态
	if !instance.IsConnected() {
		logger.Warn("Instance is not connected",
			zap.Int("instance_id", instance.id),
			zap.String("conn_id", instance.connID))
		return false
	}

	return true
}

// startPoolHealthMonitor 启动连接池健康监控
func (p *WebSocketPool) startPoolHealthMonitor(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (p *WebSocketPool) performHealthCheck() {
	p.mu.Lock()
	defer p.mu.Unlock()

	unhealthyInstances := make([]*PooledWebSocket, 0)

	for _, instance := range p.instances {
		if !p.isInstanceHealthy(instance) {
			unhealthyInstances = append(unhealthyInstances, instance)
		}
	}

	// 处理不健康的实例
	for _, instance := range unhealthyInstances {
		p.handleUnhealthyInstance(instance)
	}
}

// handleUnhealthyInstance 处理不健康的实例
func (p *WebSocketPool) handleUnhealthyInstance(instance *PooledWebSocket) {
	logger.Warn("Handling unhealthy instance",
		zap.Int("instance_id", instance.id),
		zap.String("conn_id", instance.connID),
		zap.Int32("reconnect_count", atomic.LoadInt32(&instance.reconnectCount)))

	// 尝试重连
	if atomic.LoadInt32(&instance.reconnectCount) < 3 {
		go p.attemptReconnect(instance)
	} else {
		// 重连次数过多，移除实例
		p.removeInstance(instance)
	}
}

// attemptReconnect 尝试重连实例
func (p *WebSocketPool) attemptReconnect(instance *PooledWebSocket) {
	atomic.AddInt32(&instance.reconnectCount, 1)

	logger.Info("Attempting to reconnect instance",
		zap.Int("instance_id", instance.id),
		zap.String("conn_id", instance.connID),
		zap.Int32("attempt", atomic.LoadInt32(&instance.reconnectCount)))

	// 先断开现有连接
	if err := instance.Disconnect(); err != nil {
		logger.Error("Failed to disconnect instance for reconnect",
			zap.Int("instance_id", instance.id),
			zap.Error(err))
	}

	// 等待一段时间后重连
	time.Sleep(5 * time.Second)

	// 尝试重新连接
	if err := instance.Connect(p.ctx); err != nil {
		logger.Error("Failed to reconnect instance",
			zap.Int("instance_id", instance.id),
			zap.Error(err))
		return
	}

	// 重连成功，重置计数器
	atomic.StoreInt32(&instance.reconnectCount, 0)
	instance.lastHealthy = time.Now()

	logger.Info("Successfully reconnected instance",
		zap.Int("instance_id", instance.id),
		zap.String("conn_id", instance.connID))
}

// removeInstance 移除实例
func (p *WebSocketPool) removeInstance(instance *PooledWebSocket) {
	logger.Warn("Removing unhealthy instance",
		zap.Int("instance_id", instance.id),
		zap.String("conn_id", instance.connID))

	// 清理symbol映射
	for symbol, mappedInstance := range p.symbolToInstance {
		if mappedInstance == instance {
			delete(p.symbolToInstance, symbol)
		}
	}

	// 从实例列表中移除
	for i, inst := range p.instances {
		if inst == instance {
			p.instances = append(p.instances[:i], p.instances[i+1:]...)
			break
		}
	}

	// 清理资源
	instance.Disconnect()
}

// GenerateChannel 生成频道名称 - 使用第一个实例
func (p *WebSocketPool) GenerateChannel(symbol string, params map[string]interface{}) string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.instances) > 0 {
		return p.instances[0].exchange.GenerateChannel(symbol, params)
	}

	// 如果没有实例，使用exchange直接生成
	return p.exchange.GenerateChannel(symbol, params)
}

// GetStats 获取连接池统计信息
func (p *WebSocketPool) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	instances := make([]map[string]interface{}, len(p.instances))
	totalSubscriptions := int32(0)
	healthyInstances := 0

	for i, instance := range p.instances {
		subs := atomic.LoadInt32(&instance.subscriptions)
		totalSubscriptions += subs

		isHealthy := p.isInstanceHealthy(instance)
		if isHealthy {
			healthyInstances++
		}

		instances[i] = map[string]interface{}{
			"id":              instance.id,
			"conn_id":         instance.connID,
			"subscriptions":   subs,
			"created_at":      instance.createdAt,
			"connected":       instance.IsConnected(),
			"healthy":         isHealthy,
			"reconnect_count": atomic.LoadInt32(&instance.reconnectCount),
			"last_healthy":    instance.lastHealthy,
		}

		// 限制器统计已移除
	}

	stats := map[string]interface{}{
		"total_instances":     len(p.instances),
		"healthy_instances":   healthyInstances,
		"total_subscriptions": totalSubscriptions,
		"max_per_instance":    maxStreamsPerInstance,
		"max_instances":       maxInstancesAllowed,
		"symbol_mappings":     len(p.symbolToInstance),
		"instances":           instances,
		"running":             p.running,
	}

	return stats
}

// Close 关闭连接池
func (p *WebSocketPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	// 取消上下文
	p.cancelFunc()
	p.running = false

	// 关闭所有实例
	for _, instance := range p.instances {
		// 断开WebSocket连接
		if err := instance.Disconnect(); err != nil {
			logger.Error("Failed to close instance",
				zap.Int("instance_id", instance.id),
				zap.String("conn_id", instance.connID),
				zap.Error(err))
		}
	}

	// 清理映射
	p.symbolToInstance = make(map[string]*PooledWebSocket)
	p.instances = nil

	logger.Info("WebSocket pool closed",
		zap.Int("instances_closed", len(p.instances)))

	return nil
}
