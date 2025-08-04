package ccxt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ========== WebSocket 基础框架 ==========

// WebSocketConnection WebSocket连接管理
type WebSocketConnection struct {
	conn           *websocket.Conn
	url            string
	isConnected    bool
	pingInterval   time.Duration
	autoReconnect  bool
	maxReconnect   int // 最大重连次数
	reconnectCount int // 当前重连次数
	mutex          sync.RWMutex

	// 消息处理
	messageHandlers map[string]func([]byte) error
	errorHandler    func(error)

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc
}

// WebSocketManager WebSocket管理器
type WebSocketManager struct {
	connections map[string]*WebSocketConnection
	mutex       sync.RWMutex
}

// NewWebSocketManager 创建WebSocket管理器
func NewWebSocketManager() *WebSocketManager {
	return &WebSocketManager{
		connections: make(map[string]*WebSocketConnection),
	}
}

// Connect 建立WebSocket连接
func (wm *WebSocketManager) Connect(ctx context.Context, url, name string) (*WebSocketConnection, error) {
	return wm.ConnectWithRetry(ctx, url, name, 3) // 默认3次重连
}

// ConnectWithRetry 建立WebSocket连接（带重连）
func (wm *WebSocketManager) ConnectWithRetry(ctx context.Context, url, name string, maxReconnect int) (*WebSocketConnection, error) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	if conn, exists := wm.connections[name]; exists && conn.isConnected {
		return conn, nil
	}

	wsConn := &WebSocketConnection{
		url:             url,
		isConnected:     false,
		pingInterval:    30 * time.Second,
		autoReconnect:   true,
		maxReconnect:    maxReconnect,
		reconnectCount:  0,
		messageHandlers: make(map[string]func([]byte) error),
	}

	// 尝试连接
	if err := wsConn.connect(ctx); err != nil {
		return nil, err
	}

	wm.connections[name] = wsConn
	return wsConn, nil
}

// connect 执行实际连接
func (ws *WebSocketConnection) connect(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, ws.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", ws.url, err)
	}

	wsCtx, cancel := context.WithCancel(ctx)

	ws.mutex.Lock()
	ws.conn = conn
	ws.isConnected = true
	ws.ctx = wsCtx
	ws.cancel = cancel
	ws.mutex.Unlock()

	// 启动协程
	go ws.messageLoop()
	go ws.pingLoop()

	return nil
}

// reconnect 重连逻辑
func (ws *WebSocketConnection) reconnect() {
	if !ws.autoReconnect || ws.reconnectCount >= ws.maxReconnect {
		return
	}

	ws.reconnectCount++

	// 指数退避：2^attempt * 1秒，最大30秒
	backoff := time.Duration(1<<uint(ws.reconnectCount)) * time.Second
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}

	time.Sleep(backoff)

	if err := ws.connect(ws.ctx); err != nil {
		if ws.errorHandler != nil {
			ws.errorHandler(fmt.Errorf("reconnect failed: %w", err))
		}
		go ws.reconnect() // 继续重连
	} else {
		ws.reconnectCount = 0 // 重连成功，重置计数
	}
}

// messageLoop 消息处理循环
func (ws *WebSocketConnection) messageLoop() {
	defer func() {
		ws.mutex.Lock()
		ws.isConnected = false
		if ws.conn != nil {
			ws.conn.Close()
		}
		ws.mutex.Unlock()

		// 如果启用重连，则尝试重连
		if ws.autoReconnect && ws.reconnectCount < ws.maxReconnect {
			go ws.reconnect()
		}
	}()

	for {
		select {
		case <-ws.ctx.Done():
			return
		default:
			_, message, err := ws.conn.ReadMessage()
			if err != nil {
				if ws.errorHandler != nil {
					ws.errorHandler(err)
				}
				return // 会触发defer中的重连逻辑
			}

			// 处理消息
			ws.handleMessage(message)
		}
	}
}

// ParsedMessage 预解析的消息结构
type ParsedMessage struct {
	Raw    []byte
	Parsed map[string]interface{}
	Error  error
}

// handleMessage 处理接收到的消息
func (ws *WebSocketConnection) handleMessage(message []byte) {
	// 预解析消息，避免重复解析
	parsedMsg := &ParsedMessage{
		Raw: message,
	}

	if err := json.Unmarshal(message, &parsedMsg.Parsed); err != nil {
		parsedMsg.Error = err
	}

	// 优先使用"all"处理器，传递预解析的消息
	if handler, exists := ws.messageHandlers["all"]; exists {
		if err := handler(message); err != nil && ws.errorHandler != nil {
			ws.errorHandler(err)
		}
		return
	}

	// 如果JSON解析失败，报错并返回
	if parsedMsg.Error != nil {
		if ws.errorHandler != nil {
			ws.errorHandler(fmt.Errorf("failed to parse message: %w", parsedMsg.Error))
		}
		return
	}

	// 查找对应的处理器
	for topic, handler := range ws.messageHandlers {
		if topic == "all" {
			continue // 已经在上面处理了
		}
		if channel, ok := parsedMsg.Parsed["channel"].(string); ok && channel == topic {
			if err := handler(message); err != nil && ws.errorHandler != nil {
				ws.errorHandler(err)
			}
			return
		}
	}
}

// pingLoop ping保活循环
func (ws *WebSocketConnection) pingLoop() {
	ticker := time.NewTicker(ws.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ws.ctx.Done():
			return
		case <-ticker.C:
			ws.mutex.Lock()
			if ws.isConnected {
				if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					if ws.errorHandler != nil {
						ws.errorHandler(err)
					}
				}
			}
			ws.mutex.Unlock()
		}
	}
}

// Subscribe 订阅主题
func (ws *WebSocketConnection) Subscribe(topic string, handler func([]byte) error) error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	ws.messageHandlers[topic] = handler

	subscribeMsg := map[string]interface{}{
		"method": "subscribe",
		"params": map[string]interface{}{
			"channel": topic,
		},
	}

	return ws.SendMessage(subscribeMsg)
}

// SetSubscribeHandler 设置订阅处理器
func (ws *WebSocketConnection) SetSubscribeHandler(topic string, handler func([]byte) error) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()
	ws.messageHandlers[topic] = handler
}

// SendMessage 发送消息
func (ws *WebSocketConnection) SendMessage(msg interface{}) error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	if !ws.isConnected {
		return fmt.Errorf("connection not established")
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return ws.conn.WriteMessage(websocket.TextMessage, data)
}

// Close 关闭连接
func (ws *WebSocketConnection) Close() error {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	ws.isConnected = false
	ws.cancel()

	if ws.conn != nil {
		return ws.conn.Close()
	}

	return nil
}
