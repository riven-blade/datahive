package ccxt

import (
	"fmt"
	"net/http"
)

// ========== 错误类型层次结构 ==========

// Error 基础错误接口
type Error interface {
	error
	GetType() string
	GetCode() int
	GetDetails() string
}

// BaseError 基础错误结构
type BaseError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details"`
	Code    int    `json:"code"`
}

func (e *BaseError) Error() string {
	return e.Message
}

func (e *BaseError) GetType() string {
	return e.Type
}

func (e *BaseError) GetCode() int {
	return e.Code
}

func (e *BaseError) GetDetails() string {
	return e.Details
}

// ========== 网络和连接错误 ==========

// NetworkError 网络错误
type NetworkError struct {
	*BaseError
}

func NewNetworkError(message string) *NetworkError {
	return &NetworkError{
		BaseError: &BaseError{
			Type:    "NetworkError",
			Message: message,
		},
	}
}

// RequestTimeout 请求超时错误
type RequestTimeout struct {
	*BaseError
}

func NewRequestTimeout(message string) *RequestTimeout {
	return &RequestTimeout{
		BaseError: &BaseError{
			Type:    "RequestTimeout",
			Message: message,
		},
	}
}

// DDoSProtection DDoS保护错误
type DDoSProtection struct {
	*BaseError
}

func NewDDoSProtection(message string) *DDoSProtection {
	return &DDoSProtection{
		BaseError: &BaseError{
			Type:    "DDoSProtection",
			Message: message,
		},
	}
}

// ExchangeNotAvailable 交易所不可用错误
type ExchangeNotAvailable struct {
	*BaseError
}

func NewExchangeNotAvailable(message string) *ExchangeNotAvailable {
	return &ExchangeNotAvailable{
		BaseError: &BaseError{
			Type:    "ExchangeNotAvailable",
			Message: message,
		},
	}
}

// ========== 认证和权限错误 ==========

// AuthenticationError 认证错误
type AuthenticationError struct {
	*BaseError
}

func NewAuthenticationError(message string) *AuthenticationError {
	return &AuthenticationError{
		BaseError: &BaseError{
			Type:    "AuthenticationError",
			Message: message,
		},
	}
}

// PermissionDenied 权限拒绝错误
type PermissionDenied struct {
	*BaseError
}

func NewPermissionDenied(message string) *PermissionDenied {
	return &PermissionDenied{
		BaseError: &BaseError{
			Type:    "PermissionDenied",
			Message: message,
			Code:    403,
		},
	}
}

// InvalidNonce 无效随机数错误
type InvalidNonce struct {
	*BaseError
}

func NewInvalidNonce(message string) *InvalidNonce {
	return &InvalidNonce{
		BaseError: &BaseError{
			Type:    "InvalidNonce",
			Message: message,
		},
	}
}

// ========== 限流错误 ==========

// RateLimitExceeded 限流错误
type RateLimitExceeded struct {
	*BaseError
	RetryAfter int // 重试等待时间（秒）
}

func NewRateLimitExceeded(message string, retryAfter int) *RateLimitExceeded {
	return &RateLimitExceeded{
		BaseError: &BaseError{
			Type:    "RateLimitExceeded",
			Message: message,
			Code:    429,
		},
		RetryAfter: retryAfter,
	}
}

// ========== 交易所业务错误 ==========

// ExchangeError 交易所一般错误
type ExchangeError struct {
	*BaseError
}

func NewExchangeError(message string) *ExchangeError {
	return &ExchangeError{
		BaseError: &BaseError{
			Type:    "ExchangeError",
			Message: message,
		},
	}
}

// MarketNotFound 市场未找到错误
type MarketNotFound struct {
	*BaseError
	Symbol string `json:"symbol"`
}

func NewMarketNotFound(symbol string) *MarketNotFound {
	return &MarketNotFound{
		BaseError: &BaseError{
			Type:    "MarketNotFound",
			Message: fmt.Sprintf("market %s not found", symbol),
		},
		Symbol: symbol,
	}
}

// InvalidSymbol 无效交易对错误
type InvalidSymbol struct {
	*BaseError
	Symbol string `json:"symbol"`
}

func NewInvalidSymbol(symbol string) *InvalidSymbol {
	return &InvalidSymbol{
		BaseError: &BaseError{
			Type:    "InvalidSymbol",
			Message: fmt.Sprintf("invalid symbol: %s", symbol),
		},
		Symbol: symbol,
	}
}

// MarketClosed 市场关闭错误
type MarketClosed struct {
	*BaseError
	Symbol string `json:"symbol"`
}

func NewMarketClosed(symbol string) *MarketClosed {
	return &MarketClosed{
		BaseError: &BaseError{
			Type:    "MarketClosed",
			Message: fmt.Sprintf("market %s is closed", symbol),
		},
		Symbol: symbol,
	}
}

// ========== 订单相关错误 ==========

// InvalidOrder 无效订单错误
type InvalidOrder struct {
	*BaseError
}

func NewInvalidOrder(message string, details string) *InvalidOrder {
	return &InvalidOrder{
		BaseError: &BaseError{
			Type:    "InvalidOrder",
			Message: message,
			Details: details,
			Code:    400,
		},
	}
}

// OrderNotFound 订单未找到错误
type OrderNotFound struct {
	*BaseError
	OrderID string `json:"orderId"`
}

func NewOrderNotFound(orderID string) *OrderNotFound {
	return &OrderNotFound{
		BaseError: &BaseError{
			Type:    "OrderNotFound",
			Message: fmt.Sprintf("order %s not found", orderID),
		},
		OrderID: orderID,
	}
}

// InsufficientFunds 余额不足错误
type InsufficientFunds struct {
	*BaseError
	Currency  string  `json:"currency"`
	Required  float64 `json:"required"`
	Available float64 `json:"available"`
}

func NewInsufficientFunds(currency string, required, available float64) *InsufficientFunds {
	return &InsufficientFunds{
		BaseError: &BaseError{
			Type:    "InsufficientFunds",
			Message: fmt.Sprintf("insufficient %s balance: required %.8f, available %.8f", currency, required, available),
		},
		Currency:  currency,
		Required:  required,
		Available: available,
	}
}

// InvalidAmount 无效数量错误
type InvalidAmount struct {
	*BaseError
	Amount float64 `json:"amount"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

func NewInvalidAmount(amount, min, max float64) *InvalidAmount {
	return &InvalidAmount{
		BaseError: &BaseError{
			Type:    "InvalidAmount",
			Message: fmt.Sprintf("invalid amount %.8f (min: %.8f, max: %.8f)", amount, min, max),
		},
		Amount: amount,
		Min:    min,
		Max:    max,
	}
}

// InvalidPrice 无效价格错误
type InvalidPrice struct {
	*BaseError
	Price float64 `json:"price"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

func NewInvalidPrice(price, min, max float64) *InvalidPrice {
	return &InvalidPrice{
		BaseError: &BaseError{
			Type:    "InvalidPrice",
			Message: fmt.Sprintf("invalid price %.8f (min: %.8f, max: %.8f)", price, min, max),
		},
		Price: price,
		Min:   min,
		Max:   max,
	}
}

// ========== 资金相关错误 ==========

// InvalidAddress 无效地址错误
type InvalidAddress struct {
	*BaseError
	Address string `json:"address"`
}

func NewInvalidAddress(address string) *InvalidAddress {
	return &InvalidAddress{
		BaseError: &BaseError{
			Type:    "InvalidAddress",
			Message: fmt.Sprintf("invalid address: %s", address),
		},
		Address: address,
	}
}

// AddressNotFound 地址未找到错误
type AddressNotFound struct {
	*BaseError
	Currency string `json:"currency"`
}

func NewAddressNotFound(currency string) *AddressNotFound {
	return &AddressNotFound{
		BaseError: &BaseError{
			Type:    "AddressNotFound",
			Message: fmt.Sprintf("deposit address not found for %s", currency),
		},
		Currency: currency,
	}
}

// InsufficientBalance 余额不足错误 (提现时)
type InsufficientBalance struct {
	*BaseError
	Currency  string  `json:"currency"`
	Required  float64 `json:"required"`
	Available float64 `json:"available"`
}

func NewInsufficientBalance(currency string, required, available float64) *InsufficientBalance {
	return &InsufficientBalance{
		BaseError: &BaseError{
			Type:    "InsufficientBalance",
			Message: fmt.Sprintf("insufficient %s balance for withdrawal: required %.8f, available %.8f", currency, required, available),
		},
		Currency:  currency,
		Required:  required,
		Available: available,
	}
}

// ========== 参数和请求错误 ==========

// BadRequest 错误请求
type BadRequest struct {
	*BaseError
}

func NewBadRequest(message string) *BadRequest {
	return &BadRequest{
		BaseError: &BaseError{
			Type:    "BadRequest",
			Message: message,
			Code:    400,
		},
	}
}

// BadSymbol 错误的交易对格式
type BadSymbol struct {
	*BaseError
	Symbol string `json:"symbol"`
}

func NewBadSymbol(symbol string) *BadSymbol {
	return &BadSymbol{
		BaseError: &BaseError{
			Type:    "BadSymbol",
			Message: fmt.Sprintf("bad symbol format: %s", symbol),
		},
		Symbol: symbol,
	}
}

// BadResponse 错误的响应
type BadResponse struct {
	*BaseError
	Response string `json:"response"`
}

func NewBadResponse(message, response string) *BadResponse {
	return &BadResponse{
		BaseError: &BaseError{
			Type:    "BadResponse",
			Message: message,
			Details: response,
		},
		Response: response,
	}
}

// ========== 功能不支持错误 ==========

// NotSupported 功能不支持错误
type NotSupported struct {
	*BaseError
	Feature string `json:"feature"`
}

func NewNotSupported(feature string) *NotSupported {
	return &NotSupported{
		BaseError: &BaseError{
			Type:    "NotSupported",
			Message: fmt.Sprintf("feature not supported: %s", feature),
		},
		Feature: feature,
	}
}

// ========== 操作错误 ==========

// CancelPending 取消操作正在进行中
type CancelPending struct {
	*BaseError
	OrderID string `json:"orderId"`
}

func NewCancelPending(orderID string) *CancelPending {
	return &CancelPending{
		BaseError: &BaseError{
			Type:    "CancelPending",
			Message: fmt.Sprintf("cancel pending for order %s", orderID),
		},
		OrderID: orderID,
	}
}

// OrderImmediatelyFillable 订单将立即成交
type OrderImmediatelyFillable struct {
	*BaseError
	OrderID string `json:"orderId"`
}

func NewOrderImmediatelyFillable(orderID string) *OrderImmediatelyFillable {
	return &OrderImmediatelyFillable{
		BaseError: &BaseError{
			Type:    "OrderImmediatelyFillable",
			Message: fmt.Sprintf("order %s would be immediately fillable", orderID),
		},
		OrderID: orderID,
	}
}

// OrderNotFillable 订单无法成交
type OrderNotFillable struct {
	*BaseError
	OrderID string `json:"orderId"`
}

func NewOrderNotFillable(orderID string) *OrderNotFillable {
	return &OrderNotFillable{
		BaseError: &BaseError{
			Type:    "OrderNotFillable",
			Message: fmt.Sprintf("order %s is not fillable", orderID),
		},
		OrderID: orderID,
	}
}

// ========== 错误工厂和处理函数 ==========

// HTTPError HTTP错误信息
type HTTPError struct {
	StatusCode int    `json:"statusCode"`
	StatusText string `json:"statusText"`
	URL        string `json:"url"`
	Method     string `json:"method"`
	Headers    string `json:"headers"`
	Body       string `json:"body"`
}

// CreateErrorFromHTTP 从HTTP响应创建错误
func createErrorFromHTTP(httpErr HTTPError, exchangeSpecificHandler func(HTTPError) Error) Error {
	// 首先尝试交易所特定的错误处理
	if exchangeSpecificHandler != nil {
		if err := exchangeSpecificHandler(httpErr); err != nil {
			return err
		}
	}

	// 通用HTTP错误处理
	switch httpErr.StatusCode {
	case http.StatusBadRequest:
		return NewBadRequest(fmt.Sprintf("HTTP %d: %s", httpErr.StatusCode, httpErr.StatusText))
	case http.StatusUnauthorized:
		return NewAuthenticationError("unauthorized access")
	case http.StatusForbidden:
		return NewPermissionDenied("forbidden access")
	case http.StatusNotFound:
		return NewExchangeError("endpoint not found")
	case http.StatusTooManyRequests:
		return NewRateLimitExceeded("too many requests", 60)
	case http.StatusInternalServerError:
		return NewExchangeError("internal server error")
	case http.StatusBadGateway:
		return NewExchangeNotAvailable("bad gateway")
	case http.StatusServiceUnavailable:
		return NewExchangeNotAvailable("service unavailable")
	case http.StatusGatewayTimeout:
		return NewRequestTimeout("gateway timeout")
	default:
		if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
			return NewBadRequest(fmt.Sprintf("HTTP %d: %s", httpErr.StatusCode, httpErr.StatusText))
		}
		if httpErr.StatusCode >= 500 {
			return NewExchangeError(fmt.Sprintf("HTTP %d: %s", httpErr.StatusCode, httpErr.StatusText))
		}
		return NewNetworkError(fmt.Sprintf("HTTP %d: %s", httpErr.StatusCode, httpErr.StatusText))
	}
}

// IsRetryable 检查错误是否可重试
func IsRetryable(err error) bool {
	switch err.(type) {
	case *NetworkError, *RequestTimeout, *DDoSProtection, *ExchangeNotAvailable:
		return true
	case *RateLimitExceeded:
		return true // 可以等待后重试
	default:
		return false
	}
}

// GetRetryDelay 获取重试延迟时间(秒)
func GetRetryDelay(err error) int {
	if rateLimitErr, ok := err.(*RateLimitExceeded); ok {
		return rateLimitErr.RetryAfter
	}
	return 1 // 默认1秒
}

// IsNetworkError 检查是否为网络错误
func IsNetworkError(err error) bool {
	switch err.(type) {
	case *NetworkError, *RequestTimeout, *DDoSProtection, *ExchangeNotAvailable:
		return true
	default:
		return false
	}
}

// IsAuthError 检查是否为认证错误
func IsAuthError(err error) bool {
	switch err.(type) {
	case *AuthenticationError, *PermissionDenied, *InvalidNonce:
		return true
	default:
		return false
	}
}

// IsOrderError 检查是否为订单相关错误
func IsOrderError(err error) bool {
	switch err.(type) {
	case *InvalidOrder, *OrderNotFound, *InsufficientFunds, *InvalidAmount, *InvalidPrice:
		return true
	default:
		return false
	}
}

// IsRateLimitError 检查是否为限流错误
func IsRateLimitError(err error) bool {
	_, ok := err.(*RateLimitExceeded)
	return ok
}

// ========== 错误分类映射 ==========

// ErrorCategory 错误分类
type ErrorCategory string

const (
	CategoryNetwork     ErrorCategory = "network"
	CategoryAuth        ErrorCategory = "auth"
	CategoryRateLimit   ErrorCategory = "rateLimit"
	CategoryOrder       ErrorCategory = "order"
	CategoryMarket      ErrorCategory = "market"
	CategoryFunding     ErrorCategory = "funding"
	CategoryExchange    ErrorCategory = "exchange"
	CategoryUnsupported ErrorCategory = "unsupported"
	CategoryGeneral     ErrorCategory = "general"
)

// GetErrorCategory 获取错误分类
func GetErrorCategory(err error) ErrorCategory {
	switch err.(type) {
	case *NetworkError, *RequestTimeout, *DDoSProtection, *ExchangeNotAvailable:
		return CategoryNetwork
	case *AuthenticationError, *PermissionDenied, *InvalidNonce:
		return CategoryAuth
	case *RateLimitExceeded:
		return CategoryRateLimit
	case *InvalidOrder, *OrderNotFound, *InsufficientFunds, *InvalidAmount, *InvalidPrice, *CancelPending, *OrderImmediatelyFillable, *OrderNotFillable:
		return CategoryOrder
	case *MarketNotFound, *InvalidSymbol, *MarketClosed, *BadSymbol:
		return CategoryMarket
	case *InvalidAddress, *AddressNotFound, *InsufficientBalance:
		return CategoryFunding
	case *ExchangeError:
		return CategoryExchange
	case *NotSupported:
		return CategoryUnsupported
	default:
		return CategoryGeneral
	}
}

// NewInvalidRequest 创建无效请求错误
func NewInvalidRequest(message string) *BaseError {
	return &BaseError{
		Type:    "InvalidRequest",
		Message: message,
		Code:    400,
	}
}
