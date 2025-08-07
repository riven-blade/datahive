package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"
	"go.uber.org/zap"
)

// UserDataManager 用户数据流管理器
type UserDataManager struct {
	mu          sync.RWMutex
	exchange    *Binance
	listenKey   string
	connection  *ccxt.WebSocketConnection
	isConnected bool

	// 回调管理
	balanceCallbacks map[string]func(*ccxt.WatchBalance) error
	orderCallbacks   map[string]func(*ccxt.WatchOrder) error

	// 控制
	ctx           context.Context
	cancel        context.CancelFunc
	keepAliveStop chan struct{}
}

// NewUserDataManager 创建用户数据管理器
func NewUserDataManager(exchange *Binance) *UserDataManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &UserDataManager{
		exchange:         exchange,
		balanceCallbacks: make(map[string]func(*ccxt.WatchBalance) error),
		orderCallbacks:   make(map[string]func(*ccxt.WatchOrder) error),
		ctx:              ctx,
		cancel:           cancel,
		keepAliveStop:    make(chan struct{}),
	}
}

// Connect 连接用户数据流
func (udm *UserDataManager) Connect(ctx context.Context) error {
	if !udm.exchange.config.RequiresAuth() {
		return ccxt.NewAuthenticationError("API credentials required for user data stream")
	}

	udm.mu.Lock()
	defer udm.mu.Unlock()

	if udm.isConnected {
		return nil
	}

	// 创建监听密钥
	listenKey, err := udm.createListenKey(ctx)
	if err != nil {
		return fmt.Errorf("failed to create listen key: %w", err)
	}
	udm.listenKey = listenKey

	// 连接用户数据流
	userStreamURL := udm.getUserStreamURL()
	conn, err := ccxt.NewWebSocketManager().ConnectWithRetry(ctx, userStreamURL, "binance-userdata", 3)
	if err != nil {
		return fmt.Errorf("failed to connect user data stream: %w", err)
	}

	udm.connection = conn
	udm.isConnected = true

	// 设置消息处理器
	udm.setupMessageHandlers()

	// 启动监听密钥保活
	go udm.keepListenKeyAlive()

	logger.Info("User data stream connected successfully",
		zap.String("listen_key", udm.listenKey[:8]+"..."))

	return nil
}

// Disconnect 断开用户数据流
func (udm *UserDataManager) Disconnect() error {
	udm.mu.Lock()
	defer udm.mu.Unlock()

	if !udm.isConnected {
		return nil
	}

	// 停止保活
	close(udm.keepAliveStop)

	// 关闭连接
	if udm.connection != nil {
		udm.connection.Close()
		udm.connection = nil
	}

	// 删除监听密钥
	if udm.listenKey != "" {
		ctx := context.Background()
		if err := udm.deleteListenKey(ctx, udm.listenKey); err != nil {
			logger.Error("Failed to delete listen key", zap.Error(err))
		}
		udm.listenKey = ""
	}

	udm.isConnected = false
	logger.Info("User data stream disconnected")

	return nil
}

// IsConnected 检查连接状态
func (udm *UserDataManager) IsConnected() bool {
	udm.mu.RLock()
	defer udm.mu.RUnlock()
	return udm.isConnected
}

// RegisterBalanceCallback 注册余额回调
func (udm *UserDataManager) RegisterBalanceCallback(id string, callback func(*ccxt.WatchBalance) error) {
	udm.mu.Lock()
	defer udm.mu.Unlock()
	udm.balanceCallbacks[id] = callback
}

// RegisterOrderCallback 注册订单回调
func (udm *UserDataManager) RegisterOrderCallback(id string, callback func(*ccxt.WatchOrder) error) {
	udm.mu.Lock()
	defer udm.mu.Unlock()
	udm.orderCallbacks[id] = callback
}

// UnregisterCallback 注销回调
func (udm *UserDataManager) UnregisterCallback(id string) {
	udm.mu.Lock()
	defer udm.mu.Unlock()
	delete(udm.balanceCallbacks, id)
	delete(udm.orderCallbacks, id)
}

// 私有方法

// createListenKey 创建监听密钥
func (udm *UserDataManager) createListenKey(ctx context.Context) (string, error) {
	url := "/api/v3/userDataStream"
	if udm.exchange.marketType == "futures" {
		url = "/fapi/v1/listenKey"
	} else if udm.exchange.marketType == "delivery" {
		url = "/dapi/v1/listenKey"
	}

	params := make(map[string]interface{})
	signedURL, headers, body, err := udm.exchange.Sign(url, "private", "POST", params, nil, nil)
	if err != nil {
		return "", err
	}

	respBody, err := udm.exchange.Fetch(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return "", err
	}

	var response struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return "", err
	}

	return response.ListenKey, nil
}

// getUserStreamURL 获取用户数据流URL
func (udm *UserDataManager) getUserStreamURL() string {
	// 测试网络
	if udm.exchange.config.TestNet {
		if udm.exchange.marketType == "futures" {
			return fmt.Sprintf("wss://fstream.binancefuture.com/ws/%s", udm.listenKey)
		}
		return fmt.Sprintf("wss://testnet.binance.vision/ws/%s", udm.listenKey)
	}

	// 生产环境
	switch udm.exchange.marketType {
	case "futures":
		return fmt.Sprintf("wss://fstream.binance.com/ws/%s", udm.listenKey)
	case "delivery":
		return fmt.Sprintf("wss://dstream.binance.com/ws/%s", udm.listenKey)
	default: // spot或其他
		return fmt.Sprintf("wss://stream.binance.com:9443/ws/%s", udm.listenKey)
	}
}

// setupMessageHandlers 设置消息处理器
func (udm *UserDataManager) setupMessageHandlers() {
	udm.connection.SetSubscribeHandler("all", udm.handleUserDataMessage)
}

// handleUserDataMessage 处理用户数据消息
func (udm *UserDataManager) handleUserDataMessage(data []byte) error {
	logger.Debug("Received user data message", zap.ByteString("data", data))

	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		logger.Error("Failed to parse user data message", zap.Error(err))
		return err
	}

	eventType, ok := msg["e"].(string)
	if !ok {
		logger.Debug("User data message without event type", zap.Any("msg", msg))
		return nil
	}

	switch eventType {
	case "outboundAccountPosition", "balanceUpdate":
		return udm.handleBalanceUpdate(data, msg)
	case "executionReport":
		return udm.handleOrderUpdate(data, msg)
	case "trade":
		return udm.handleTradeUpdate(data, msg)
	default:
		logger.Debug("Unknown user data event type", zap.String("event", eventType))
	}

	return nil
}

// handleBalanceUpdate 处理余额更新
func (udm *UserDataManager) handleBalanceUpdate(rawData []byte, msg map[string]interface{}) error {
	// 解析余额数据
	binanceInstance := &Binance{}
	account := binanceInstance.parseAccountBalance(msg)
	if account == nil {
		return fmt.Errorf("failed to parse balance data")
	}

	watchBalance := &ccxt.WatchBalance{
		Account:    *account,
		StreamName: "user_balance",
	}

	// 调用所有注册的回调
	udm.mu.RLock()
	callbacks := make([]func(*ccxt.WatchBalance) error, 0, len(udm.balanceCallbacks))
	for _, callback := range udm.balanceCallbacks {
		callbacks = append(callbacks, callback)
	}
	udm.mu.RUnlock()

	for _, callback := range callbacks {
		if err := callback(watchBalance); err != nil {
			logger.Error("Balance callback error", zap.Error(err))
		}
	}

	return nil
}

// handleOrderUpdate 处理订单更新
func (udm *UserDataManager) handleOrderUpdate(rawData []byte, msg map[string]interface{}) error {
	// 解析订单数据
	binanceInstance := &Binance{}
	order := binanceInstance.parseOrderUpdate(msg)
	if order == nil {
		return fmt.Errorf("failed to parse order data")
	}

	watchOrder := &ccxt.WatchOrder{
		Order:      *order,
		StreamName: "user_orders",
	}

	// 调用所有注册的回调
	udm.mu.RLock()
	callbacks := make([]func(*ccxt.WatchOrder) error, 0, len(udm.orderCallbacks))
	for _, callback := range udm.orderCallbacks {
		callbacks = append(callbacks, callback)
	}
	udm.mu.RUnlock()

	for _, callback := range callbacks {
		if err := callback(watchOrder); err != nil {
			logger.Error("Order callback error", zap.Error(err))
		}
	}

	return nil
}

// handleTradeUpdate 处理交易更新
func (udm *UserDataManager) handleTradeUpdate(rawData []byte, msg map[string]interface{}) error {
	// 解析交易数据 - 这通常是私有交易数据
	// 实现类似于公共交易数据的解析，但包含用户特定信息
	logger.Debug("User trade update received", zap.Any("msg", msg))

	// TODO: 实现用户交易数据解析
	// 这需要根据具体的Binance用户交易数据格式来实现

	return nil
}

// keepListenKeyAlive 保持监听密钥活跃
func (udm *UserDataManager) keepListenKeyAlive() {
	ticker := time.NewTicker(30 * time.Minute) // 每30分钟更新一次
	defer ticker.Stop()

	for {
		select {
		case <-udm.ctx.Done():
			return
		case <-udm.keepAliveStop:
			return
		case <-ticker.C:
			if err := udm.extendListenKey(udm.ctx, udm.listenKey); err != nil {
				logger.Error("Failed to extend listen key", zap.Error(err))
				// 可以在这里实现重连逻辑
			} else {
				logger.Debug("Listen key extended successfully")
			}
		}
	}
}

// extendListenKey 延长监听密钥
func (udm *UserDataManager) extendListenKey(ctx context.Context, listenKey string) error {
	url := "/api/v3/userDataStream"
	if udm.exchange.marketType == "futures" {
		url = "/fapi/v1/listenKey"
	} else if udm.exchange.marketType == "delivery" {
		url = "/dapi/v1/listenKey"
	}

	params := map[string]interface{}{
		"listenKey": listenKey,
	}

	signedURL, headers, body, err := udm.exchange.Sign(url, "private", "PUT", params, nil, nil)
	if err != nil {
		return err
	}

	_, err = udm.exchange.Fetch(ctx, signedURL, "PUT", headers, fmt.Sprintf("%v", body))
	return err
}

// deleteListenKey 删除监听密钥
func (udm *UserDataManager) deleteListenKey(ctx context.Context, listenKey string) error {
	url := "/api/v3/userDataStream"
	if udm.exchange.marketType == "futures" {
		url = "/fapi/v1/listenKey"
	} else if udm.exchange.marketType == "delivery" {
		url = "/dapi/v1/listenKey"
	}

	params := map[string]interface{}{
		"listenKey": listenKey,
	}

	signedURL, headers, body, err := udm.exchange.Sign(url, "private", "DELETE", params, nil, nil)
	if err != nil {
		return err
	}

	_, err = udm.exchange.Fetch(ctx, signedURL, "DELETE", headers, fmt.Sprintf("%v", body))
	return err
}

// Close 关闭用户数据管理器
func (udm *UserDataManager) Close() error {
	udm.cancel()
	return udm.Disconnect()
}
