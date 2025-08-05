package protocol

// =============================================================================
// Protocol Information Constants
// =============================================================================

const (
	// Protocol identification
	ProtocolSemanticVersion = "2.0"      // Protobuf版本
	ProtocolName            = "BrainBot" // Protocol name

	// Protocol capabilities
	SupportsBinaryEncoding = true
	SupportsProtobuf       = true
	SupportsHeartbeat      = true
)

// =============================================================================
// Protocol Error Codes
// =============================================================================

// Protocol error codes (协议层错误代码)
const (
	// Protocol level errors (1xxx)
	ErrCodeInvalidRequest     = "INVALID_REQUEST"
	ErrCodeProtocolError      = "PROTOCOL_ERROR"
	ErrCodeMessageTooLarge    = "MESSAGE_TOO_LARGE"
	ErrCodeInvalidMessage     = "INVALID_MESSAGE"
	ErrCodeUnsupportedVersion = "UNSUPPORTED_VERSION"

	// Authentication and authorization (2xxx)
	ErrCodeAuthRequired     = "AUTH_REQUIRED"
	ErrCodeAuthFailed       = "AUTH_FAILED"
	ErrCodePermissionDenied = "PERMISSION_DENIED"
	ErrCodeTokenExpired     = "TOKEN_EXPIRED"

	// Business logic errors (3xxx)
	ErrCodeSymbolNotFound   = "SYMBOL_NOT_FOUND"
	ErrCodeInvalidSymbol    = "INVALID_SYMBOL"
	ErrCodeMarketClosed     = "MARKET_CLOSED"
	ErrCodeDataNotAvailable = "DATA_NOT_AVAILABLE"

	// Server errors (5xxx)
	ErrCodeInternalError = "INTERNAL_ERROR"
	ErrCodeServiceBusy   = "SERVICE_BUSY"
	ErrCodeTimeout       = "TIMEOUT"
	ErrCodeRateLimit     = "RATE_LIMIT"
)

// =============================================================================
// WebSocket Stream Event Types
// =============================================================================

// Stream event type constants for WebSocket subscriptions
const (
	StreamEventKline      = "kline"       // K线数据流
	StreamEventTrade      = "trade"       // 交易数据流
	StreamEventOrderBook  = "order_book"  // 订单簿数据流
	StreamEventMiniTicker = "mini_ticker" // 轻量级ticker数据流
	StreamEventBookTicker = "book_ticker" // 最优买卖价数据流
	StreamEventMarkPrice  = "mark_price"  // 标记价格数据流
	StreamEventBalance    = "balance"     // 账户余额数据流
	StreamEventOrders     = "orders"      // 订单数据流
)

// All supported stream event types for iteration
var AllStreamEventTypes = []string{
	StreamEventKline,
	StreamEventTrade,
	StreamEventOrderBook,
	StreamEventMiniTicker,
	StreamEventBookTicker,
	StreamEventMarkPrice,
	StreamEventBalance,
	StreamEventOrders,
}

// IsValidStreamEvent 检查事件类型是否有效
func IsValidStreamEvent(eventType string) bool {
	for _, validType := range AllStreamEventTypes {
		if validType == eventType {
			return true
		}
	}
	return false
}

// GetMarketDataStreamEvents 获取所有市场数据流事件类型（排除账户相关）
func GetMarketDataStreamEvents() []string {
	return []string{
		StreamEventKline,
		StreamEventTrade,
		StreamEventOrderBook,
		StreamEventMiniTicker,
		StreamEventBookTicker,
		StreamEventMarkPrice,
	}
}

// GetAccountStreamEvents 获取所有账户相关流事件类型
func GetAccountStreamEvents() []string {
	return []string{
		StreamEventBalance,
		StreamEventOrders,
	}
}
