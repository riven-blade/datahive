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
