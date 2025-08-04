package ccxt

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ========== 配置和常量 ==========

// DefaultConfig 创建默认配置
func DefaultConfig() ExchangeConfig {
	return NewDefaultExchangeConfig()
}

// BaseExchange 基础交易所实现
type BaseExchange struct {
	// ========== 基础配置 ==========
	id        string
	name      string
	countries []string
	version   string
	certified bool
	pro       bool

	// ========== API 配置 ==========
	apiKey   string
	secret   string
	password string
	uid      string

	// ========== 网络配置 ==========
	sandbox         bool
	testnet         bool
	timeout         time.Duration
	rateLimit       int
	enableRateLimit bool
	httpProxy       string
	httpsProxy      string
	socksProxy      string
	userAgent       string
	headers         map[string]string

	// ========== 精度配置 ==========
	precisionMode int
	paddingMode   int

	// ========== 数据存储 ==========
	markets        map[string]*Market
	currencies     map[string]*Currency
	marketsById    map[string]*Market
	currenciesById map[string]*Currency

	// ========== 功能支持 ==========
	has        map[string]bool
	timeframes map[string]string

	// ========== URL 配置 ==========
	urls map[string]interface{}
	api  map[string]interface{}

	// ========== 费率配置 ==========
	fees        map[string]map[string]interface{}
	tradingFees map[string]*TradingFee
	fundingFees map[string]*Currency

	// ========== 运行时状态 ==========
	httpClient      *http.Client
	lastRequestTime int64
	requestCount    int64

	// ========== 简化重试配置 ==========
	maxRetries    int
	retryDelay    time.Duration
	maxRetryDelay time.Duration
	enableJitter  bool

	// ========== 选项配置 ==========
	options map[string]interface{}

	// ========== 同步锁 ==========
	mutex sync.RWMutex
}

// ========== 重试配置方法 ==========

// SetRetryConfig 设置重试配置
func (b *BaseExchange) SetRetryConfig(maxRetries int, retryDelay, maxRetryDelay time.Duration, enableJitter bool) {
	b.maxRetries = maxRetries
	b.retryDelay = retryDelay
	b.maxRetryDelay = maxRetryDelay
	b.enableJitter = enableJitter
}

// EnableRetry 启用重试机制
func (b *BaseExchange) EnableRetry() {
	if b.maxRetries == 0 {
		b.maxRetries = 3 // 默认重试3次
	}
	if b.retryDelay == 0 {
		b.retryDelay = 100 * time.Millisecond // 默认100ms
	}
	if b.maxRetryDelay == 0 {
		b.maxRetryDelay = 10 * time.Second // 默认最大10秒
	}
}

// DisableRetry 禁用重试机制
func (b *BaseExchange) DisableRetry() {
	b.maxRetries = 0
}

// ========== 重试逻辑实现 ==========

// shouldRetry 判断错误是否应该重试
func (b *BaseExchange) shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// 网络错误，应该重试
	if _, ok := err.(*NetworkError); ok {
		return true
	}

	// 交易所不可用，应该重试
	if _, ok := err.(*ExchangeNotAvailable); ok {
		return true
	}

	// 限流错误，应该重试
	if _, ok := err.(*RateLimitExceeded); ok {
		return true
	}

	// 请求超时，应该重试
	if _, ok := err.(*RequestTimeout); ok {
		return true
	}

	// 检查具体的HTTP相关错误类型
	switch err.(type) {
	case *RateLimitExceeded:
		return true // 429 Too Many Requests
	case *ExchangeNotAvailable:
		return true // 502, 503, 504等
	}

	// 检查错误消息中的关键词
	errMsg := strings.ToLower(err.Error())
	retryableKeywords := []string{
		"connection", "timeout", "network", "temporary",
		"unavailable", "overloaded", "rate limit",
		"too many requests", "service unavailable",
		"bad gateway", "gateway timeout",
	}

	for _, keyword := range retryableKeywords {
		if strings.Contains(errMsg, keyword) {
			return true
		}
	}

	return false
}

// calculateBackoffDelay 计算退避延迟
func (b *BaseExchange) calculateBackoffDelay(attempt int) time.Duration {
	// 指数退避：baseDelay * 2^attempt
	delay := time.Duration(float64(b.retryDelay) * math.Pow(2, float64(attempt)))

	// 限制最大延迟
	if delay > b.maxRetryDelay {
		delay = b.maxRetryDelay
	}

	// 添加随机抖动以避免惊群效应
	if b.enableJitter && attempt > 0 {
		jitterRange := float64(delay) * 0.1                // 10%的抖动范围
		jitter := (rand.Float64() - 0.5) * 2 * jitterRange // -10% 到 +10%
		delay = time.Duration(float64(delay) + jitter)

		// 确保延迟不会为负数
		if delay < 0 {
			delay = b.retryDelay
		}
	}

	return delay
}

// RetryWithBackoff 执行带指数退避的重试
func (b *BaseExchange) RetryWithBackoff(ctx context.Context, operation func() error) error {
	if b.maxRetries == 0 {
		return operation()
	}

	var lastErr error
	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		// 执行操作
		lastErr = operation()
		if lastErr == nil {
			return nil // 成功
		}

		// 检查是否应该重试
		if !b.shouldRetry(lastErr) {
			return lastErr // 不应重试的错误，直接返回
		}

		// 最后一次尝试失败，不再重试
		if attempt >= b.maxRetries {
			break
		}

		// 计算退避延迟
		backoffDelay := b.calculateBackoffDelay(attempt)

		// 等待退避时间
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffDelay):
			// 继续下一次重试
		}
	}

	return fmt.Errorf("operation failed after %d retries: %w", b.maxRetries, lastErr)
}

// RetryWithBackoffAndResult 执行带指数退避的重试，并返回结果
func (b *BaseExchange) RetryWithBackoffAndResult(ctx context.Context, operation func() (interface{}, error)) (interface{}, error) {
	if b.maxRetries == 0 {
		return operation()
	}

	var lastErr error
	var result interface{}

	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		// 执行操作
		result, lastErr = operation()
		if lastErr == nil {
			return result, nil // 成功
		}

		// 检查是否应该重试
		if !b.shouldRetry(lastErr) {
			return nil, lastErr // 不应重试的错误，直接返回
		}

		// 最后一次尝试失败，不再重试
		if attempt >= b.maxRetries {
			break
		}

		// 计算退避延迟
		backoffDelay := b.calculateBackoffDelay(attempt)

		// 等待退避时间
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoffDelay):
			// 继续下一次重试
		}
	}

	return nil, fmt.Errorf("operation failed after %d retries: %w", b.maxRetries, lastErr)
}

// ========== 构造函数 ==========

// NewBaseExchange 创建基础交易所实例
func NewBaseExchange(id, name, version string, countries []string) *BaseExchange {
	base := &BaseExchange{
		id:              id,
		name:            name,
		version:         version,
		countries:       countries,
		timeout:         30 * time.Second,
		rateLimit:       1000,
		enableRateLimit: true,
		userAgent:       "datahive/1.0.0",
		headers:         make(map[string]string),
		markets:         make(map[string]*Market),
		currencies:      make(map[string]*Currency),
		marketsById:     make(map[string]*Market),
		currenciesById:  make(map[string]*Currency),
		has:             make(map[string]bool),
		timeframes:      make(map[string]string),
		fees:            make(map[string]map[string]interface{}),
		tradingFees:     make(map[string]*TradingFee),
		fundingFees:     make(map[string]*Currency),
		options:         make(map[string]interface{}),
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		maxRetries:      3,
		retryDelay:      100 * time.Millisecond,
		maxRetryDelay:   10 * time.Second,
		enableJitter:    true,
	}

	// 设置默认功能支持
	base.setDefaultCapabilities()
	base.setDefaultTimeframes()

	return base
}

// setDefaultCapabilities 设置默认功能支持
func (b *BaseExchange) setDefaultCapabilities() {
	b.has["fetchMarkets"] = true
	b.has["fetchTicker"] = true
	b.has["fetchOHLCV"] = true
	b.has["fetchTrades"] = true
	b.has["fetchOrderBook"] = true
	b.has["fetchBalance"] = false
	b.has["createOrder"] = false
	b.has["cancelOrder"] = false
	b.has["fetchOrder"] = false
	b.has["fetchOrders"] = false
	b.has["fetchOpenOrders"] = false
	b.has["fetchClosedOrders"] = false
	b.has["fetchMyTrades"] = false
	b.has["fetchPositions"] = false
	b.has["fetchFundingRate"] = false
	b.has["setLeverage"] = false
	b.has["setMarginMode"] = false
}

// setDefaultTimeframes 设置默认时间周期
func (b *BaseExchange) setDefaultTimeframes() {
	b.timeframes["1m"] = "1m"
	b.timeframes["3m"] = "3m"
	b.timeframes["5m"] = "5m"
	b.timeframes["15m"] = "15m"
	b.timeframes["30m"] = "30m"
	b.timeframes["1h"] = "1h"
	b.timeframes["2h"] = "2h"
	b.timeframes["4h"] = "4h"
	b.timeframes["6h"] = "6h"
	b.timeframes["8h"] = "8h"
	b.timeframes["12h"] = "12h"
	b.timeframes["1d"] = "1d"
	b.timeframes["3d"] = "3d"
	b.timeframes["1w"] = "1w"
	b.timeframes["1M"] = "1M"
}

// ========== 基础信息方法 ==========

func (b *BaseExchange) GetID() string              { return b.id }
func (b *BaseExchange) GetName() string            { return b.name }
func (b *BaseExchange) GetCountries() []string     { return b.countries }
func (b *BaseExchange) GetVersion() string         { return b.version }
func (b *BaseExchange) GetRateLimit() int          { return b.rateLimit }
func (b *BaseExchange) GetCertifiedAPIKey() string { return "" } // 子类实现
func (b *BaseExchange) GetTimeout() int            { return int(b.timeout / time.Second) }
func (b *BaseExchange) GetSandbox() bool           { return b.sandbox }
func (b *BaseExchange) GetUserAgent() string       { return b.userAgent }
func (b *BaseExchange) GetProxy() string           { return b.httpProxy }
func (b *BaseExchange) GetApiKey() string          { return b.apiKey }
func (b *BaseExchange) GetSecret() string          { return b.secret }
func (b *BaseExchange) GetPassword() string        { return b.password }
func (b *BaseExchange) GetUID() string             { return b.uid }

// 功能支持检查
func (b *BaseExchange) Has() map[string]bool {
	return b.has
}

func (b *BaseExchange) HasAPI(method string) bool {
	if val, exists := b.has[method]; exists {
		return val
	}
	return false
}

// 时间周期
func (b *BaseExchange) GetTimeframes() map[string]string {
	return b.timeframes
}

// 市场类型
func (b *BaseExchange) GetMarketTypes() []string {
	types := []string{}
	if b.HasAPI("spot") {
		types = append(types, MarketTypeSpot)
	}
	if b.HasAPI("margin") {
		types = append(types, MarketTypeMargin)
	}
	if b.HasAPI("swap") {
		types = append(types, MarketTypeSwap)
	}
	if b.HasAPI("future") {
		types = append(types, MarketTypeFuture)
	}
	if b.HasAPI("option") {
		types = append(types, MarketTypeOption)
	}
	return types
}

// 精度配置
func (b *BaseExchange) GetPrecisionMode() int { return b.precisionMode }
func (b *BaseExchange) GetPaddingMode() int   { return b.paddingMode }

// 费率信息
func (b *BaseExchange) GetTradingFees() map[string]map[string]interface{} {
	return b.fees
}

func (b *BaseExchange) GetFundingFees() map[string]map[string]interface{} {
	return b.fees
}

// ========== 时间处理方法 ==========

func (b *BaseExchange) Milliseconds() int64 {
	return time.Now().UnixMilli()
}

func (b *BaseExchange) Seconds() int64 {
	return time.Now().Unix()
}

func (b *BaseExchange) Microseconds() int64 {
	return time.Now().UnixMicro()
}

func (b *BaseExchange) ISO8601(timestamp int64) string {
	return time.Unix(timestamp/1000, (timestamp%1000)*1000000).UTC().Format("2006-01-02T15:04:05.000Z")
}

func (b *BaseExchange) ParseDate(dateString string) int64 {
	// 支持多种时间格式
	formats := []string{
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateString); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

func (b *BaseExchange) YMD(timestamp int64, infix string) string {
	t := time.Unix(timestamp/1000, 0).UTC()
	return fmt.Sprintf("%04d%s%02d%s%02d", t.Year(), infix, int(t.Month()), infix, t.Day())
}

// ========== 安全数据提取方法 ==========

func (b *BaseExchange) SafeString(obj map[string]interface{}, key string, defaultValue string) string {
	if val, exists := obj[key]; exists {
		if str, ok := val.(string); ok {
			return str
		}
		// 尝试转换其他类型
		return fmt.Sprintf("%v", val)
	}
	return defaultValue
}

func (b *BaseExchange) SafeStringLower(obj map[string]interface{}, key string, defaultValue string) string {
	return strings.ToLower(b.SafeString(obj, key, defaultValue))
}

func (b *BaseExchange) SafeStringUpper(obj map[string]interface{}, key string, defaultValue string) string {
	return strings.ToUpper(b.SafeString(obj, key, defaultValue))
}

func (b *BaseExchange) SafeFloat(obj map[string]interface{}, key string, defaultValue float64) float64 {
	if val, exists := obj[key]; exists {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	}
	return defaultValue
}

func (b *BaseExchange) SafeInteger(obj map[string]interface{}, key string, defaultValue int64) int64 {
	if val, exists := obj[key]; exists {
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
	}
	return defaultValue
}

func (b *BaseExchange) SafeBool(obj map[string]interface{}, key string, defaultValue bool) bool {
	if val, exists := obj[key]; exists {
		if b, ok := val.(bool); ok {
			return b
		}
		// 尝试从字符串转换
		if str, ok := val.(string); ok {
			return strings.ToLower(str) == "true" || str == "1"
		}
	}
	return defaultValue
}

func (b *BaseExchange) SafeValue(obj map[string]interface{}, key string, defaultValue interface{}) interface{} {
	if val, exists := obj[key]; exists {
		return val
	}
	return defaultValue
}

// SafeInt 安全获取整数值
func (b *BaseExchange) SafeInt(data map[string]interface{}, key string, defaultValue int64) int64 {
	if value, exists := data[key]; exists {
		switch v := value.(type) {
		case int:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				return parsed
			}
		}
	}
	return defaultValue
}

// FloatToPrecision 浮点数精度转换
func (b *BaseExchange) FloatToPrecision(value float64, precision int) string {
	format := fmt.Sprintf("%%.%df", precision)
	return fmt.Sprintf(format, value)
}

// ========== 精度处理方法 ==========

func (b *BaseExchange) PrecisionFromString(precision string) float64 {
	if f, err := strconv.ParseFloat(precision, 64); err == nil {
		return f
	}
	return 0
}

func (b *BaseExchange) DecimalToPrecision(x float64, precision int, precisionMode, paddingMode int) string {
	switch precisionMode {
	case PrecisionModeDecimalPlaces:
		format := fmt.Sprintf("%%.%df", precision)
		result := fmt.Sprintf(format, x)
		if paddingMode == PaddingModeNone {
			// 移除尾随零
			result = strings.TrimRight(result, "0")
			result = strings.TrimRight(result, ".")
		}
		return result

	case PrecisionModeSignificantDigits:
		format := fmt.Sprintf("%%.%dg", precision)
		return fmt.Sprintf(format, x)

	case PrecisionModeTickSize:
		if precision > 0 {
			tickSize := math.Pow(10, -float64(precision))
			rounded := math.Round(x/tickSize) * tickSize
			return strconv.FormatFloat(rounded, 'f', -1, 64)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)

	default:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
}

func (b *BaseExchange) AmountToPrecision(symbol string, amount float64) float64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if market, exists := b.markets[symbol]; exists {
		precision := int(market.Precision.Amount)
		formatted := b.DecimalToPrecision(amount, precision, b.precisionMode, b.paddingMode)
		if result, err := strconv.ParseFloat(formatted, 64); err == nil {
			return result
		}
	}
	return amount
}

func (b *BaseExchange) PriceToPrecision(symbol string, price float64) float64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if market, exists := b.markets[symbol]; exists {
		precision := int(market.Precision.Price)
		formatted := b.DecimalToPrecision(price, precision, b.precisionMode, b.paddingMode)
		if result, err := strconv.ParseFloat(formatted, 64); err == nil {
			return result
		}
	}
	return price
}

func (b *BaseExchange) CostToPrecision(symbol string, cost float64) float64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if market, exists := b.markets[symbol]; exists {
		precision := int(market.Precision.Cost)
		formatted := b.DecimalToPrecision(cost, precision, b.precisionMode, b.paddingMode)
		if result, err := strconv.ParseFloat(formatted, 64); err == nil {
			return result
		}
	}
	return cost
}

func (b *BaseExchange) FeeToPrecision(currency string, fee float64) float64 {
	// 手续费通常使用更高精度
	formatted := b.DecimalToPrecision(fee, 8, b.precisionMode, b.paddingMode)
	if result, err := strconv.ParseFloat(formatted, 64); err == nil {
		return result
	}
	return fee
}

func (b *BaseExchange) CurrencyToPrecision(currency string, amount float64) float64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if curr, exists := b.currencies[currency]; exists {
		precision := curr.Precision
		formatted := b.DecimalToPrecision(amount, precision, b.precisionMode, b.paddingMode)
		if result, err := strconv.ParseFloat(formatted, 64); err == nil {
			return result
		}
	}
	return amount
}

// ========== URL和参数处理 ==========

func (b *BaseExchange) ImplodeParams(path string, params map[string]interface{}) string {
	result := path
	for key, value := range params {
		placeholder := "{" + key + "}"
		if strings.Contains(result, placeholder) {
			result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", value))
			delete(params, key)
		}
	}
	return result
}

func (b *BaseExchange) ExtractParams(path string) (string, map[string]interface{}) {
	params := make(map[string]interface{})
	re := regexp.MustCompile(`\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(path, -1)

	for _, match := range matches {
		if len(match) > 1 {
			params[match[1]] = nil
		}
	}

	return path, params
}

// ========== 符号转换 ==========

func (b *BaseExchange) MarketID(symbol string) string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if market, exists := b.markets[symbol]; exists {
		return market.ID
	}
	return symbol
}

func (b *BaseExchange) MarketSymbol(id string) string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if market, exists := b.marketsById[id]; exists {
		return market.Symbol
	}
	return id
}

// ========== HTTP 请求方法 ==========

// Request 发送HTTP请求
func (b *BaseExchange) Request(ctx context.Context, url string, method string, headers map[string]string, body interface{}, params map[string]interface{}) (*Response, error) {
	// 转换body为字符串
	var bodyStr string
	if body != nil {
		if str, ok := body.(string); ok {
			bodyStr = str
		} else if bytes, ok := body.([]byte); ok {
			bodyStr = string(bytes)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(bodyStr))
	if err != nil {
		return nil, err
	}

	// 设置默认头部
	req.Header.Set("User-Agent", b.userAgent)
	if bodyStr != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// 设置自定义头部
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// 使用HTTP客户端
	httpResp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, NewNetworkError("HTTP request failed")
	}

	// 转换为我们的Response类型
	response := &Response{
		StatusCode: httpResp.StatusCode,
		Body:       make([]byte, 0),
		Headers:    make(map[string]string),
	}

	// 复制headers
	for k, v := range httpResp.Header {
		if len(v) > 0 {
			response.Headers[k] = v[0]
		}
	}

	// 读取body
	if httpResp.Body != nil {
		defer httpResp.Body.Close()
		bodyBytes, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return nil, NewNetworkError("failed to read response body")
		}
		response.Body = bodyBytes
	}

	return response, nil
}

// RequestWithRetry 发送带重试的HTTP请求
func (b *BaseExchange) RequestWithRetry(ctx context.Context, url string, method string, headers map[string]string, body string) (*http.Response, error) {
	var resp *Response

	err := b.RetryWithBackoff(ctx, func() error {
		var reqErr error
		resp, reqErr = b.Request(ctx, url, method, headers, body, nil)
		if reqErr != nil {
			return reqErr
		}

		// 检查HTTP状态码，某些状态码需要重试
		if resp != nil {
			switch resp.StatusCode {
			case 429: // Too Many Requests
				return NewRateLimitExceeded("rate limit exceeded", 60)
			case 502, 503, 504: // Bad Gateway, Service Unavailable, Gateway Timeout
				return NewExchangeNotAvailable("exchange temporarily unavailable")
			case 500: // Internal Server Error (某些情况下可重试)
				return NewExchangeNotAvailable("internal server error")
			}
		}

		return nil
	})

	// 转换回http.Response (为了兼容)
	if resp != nil {
		httpResp := &http.Response{
			StatusCode: resp.StatusCode,
			Header:     make(http.Header),
		}
		for k, v := range resp.Headers {
			httpResp.Header.Set(k, v)
		}
		return httpResp, err
	}
	return nil, err
}

// Fetch 发送HTTP请求并处理响应
func (b *BaseExchange) Fetch(ctx context.Context, url, method string, headers map[string]string, body string) (string, error) {
	resp, err := b.Request(ctx, url, method, headers, body, nil)
	if err != nil {
		return "", err
	}

	// 读取响应体
	return string(resp.Body), nil
}

// FetchWithRetry 发送带重试的HTTP请求并处理响应
func (b *BaseExchange) FetchWithRetry(ctx context.Context, url, method string, headers map[string]string, body string) (string, error) {

	resp, err := b.RequestWithRetry(ctx, url, method, headers, body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 读取响应体
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(bodyBytes), nil
}

// ========== 默认实现方法 (子类可重写) ==========

func (b *BaseExchange) LoadMarkets(ctx context.Context, reload ...bool) (map[string]*Market, error) {
	return nil, NewNotSupported("LoadMarkets")
}

func (b *BaseExchange) FetchMarkets(ctx context.Context, params map[string]interface{}) ([]*Market, error) {
	return nil, NewNotSupported("FetchMarkets")
}

func (b *BaseExchange) FetchCurrencies(ctx context.Context, params map[string]interface{}) (map[string]*Currency, error) {
	return nil, NewNotSupported("FetchCurrencies")
}

func (b *BaseExchange) FetchTicker(ctx context.Context, symbol string, params map[string]interface{}) (*Ticker, error) {
	return nil, NewNotSupported("FetchTicker")
}

func (b *BaseExchange) FetchTickers(ctx context.Context, symbols []string, params map[string]interface{}) (map[string]*Ticker, error) {
	// 默认实现：逐个获取ticker
	result := make(map[string]*Ticker)
	for _, symbol := range symbols {
		ticker, err := b.FetchTicker(ctx, symbol, params)
		if err != nil {
			return nil, err
		}
		result[symbol] = ticker
	}
	return result, nil
}

func (b *BaseExchange) FetchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (*OrderBook, error) {
	return nil, NewNotSupported("FetchOrderBook")
}

func (b *BaseExchange) FetchL2OrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (*OrderBook, error) {
	return b.FetchOrderBook(ctx, symbol, limit, params)
}

func (b *BaseExchange) FetchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Trade, error) {
	return nil, NewNotSupported("FetchTrades")
}

func (b *BaseExchange) FetchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) ([]*OHLCV, error) {
	return nil, NewNotSupported("FetchOHLCV")
}

func (b *BaseExchange) FetchStatus(ctx context.Context, params map[string]interface{}) (*ExchangeStatus, error) {
	// 默认返回正常状态
	return &ExchangeStatus{
		Status:  "ok",
		Updated: b.Milliseconds(),
	}, nil
}

func (b *BaseExchange) FetchTime(ctx context.Context, params map[string]interface{}) (*ExchangeTime, error) {
	timestamp := b.Milliseconds()
	return &ExchangeTime{
		Timestamp: timestamp,
		Datetime:  b.ISO8601(timestamp),
	}, nil
}

// 认证方法的默认实现
func (b *BaseExchange) FetchBalance(ctx context.Context, params map[string]interface{}) (*Account, error) {
	return nil, NewNotSupported("FetchBalance")
}

func (b *BaseExchange) FetchTradingFee(ctx context.Context, symbol string, params map[string]interface{}) (*TradingFee, error) {
	return nil, NewNotSupported("FetchTradingFee")
}

func (b *BaseExchange) FetchTradingFees(ctx context.Context, params map[string]interface{}) (map[string]*TradingFee, error) {
	return nil, NewNotSupported("FetchTradingFees")
}

func (b *BaseExchange) FetchFundingFee(ctx context.Context, code string, params map[string]interface{}) (*Currency, error) {
	return nil, NewNotSupported("FetchFundingFee")
}

func (b *BaseExchange) FetchFundingFees(ctx context.Context, codes []string, params map[string]interface{}) (map[string]*Currency, error) {
	return nil, NewNotSupported("FetchFundingFees")
}

// 交易方法的默认实现
func (b *BaseExchange) CreateOrder(ctx context.Context, symbol, orderType, side string, amount, price float64, params map[string]interface{}) (*Order, error) {
	return nil, NewNotSupported("CreateOrder")
}

func (b *BaseExchange) CreateMarketOrder(ctx context.Context, symbol, side string, amount float64, params map[string]interface{}) (*Order, error) {
	return b.CreateOrder(ctx, symbol, OrderTypeMarket, side, amount, 0, params)
}

func (b *BaseExchange) CreateLimitOrder(ctx context.Context, symbol, side string, amount, price float64, params map[string]interface{}) (*Order, error) {
	return b.CreateOrder(ctx, symbol, OrderTypeLimit, side, amount, price, params)
}

func (b *BaseExchange) CreateStopOrder(ctx context.Context, symbol, side string, amount, price, stopPrice float64, params map[string]interface{}) (*Order, error) {
	if params == nil {
		params = make(map[string]interface{})
	}
	params["stopPrice"] = stopPrice
	return b.CreateOrder(ctx, symbol, OrderTypeStopMarket, side, amount, price, params)
}

func (b *BaseExchange) CreateStopLimitOrder(ctx context.Context, symbol, side string, amount, price, stopPrice float64, params map[string]interface{}) (*Order, error) {
	if params == nil {
		params = make(map[string]interface{})
	}
	params["stopPrice"] = stopPrice
	return b.CreateOrder(ctx, symbol, OrderTypeStopLimit, side, amount, price, params)
}

func (b *BaseExchange) EditOrder(ctx context.Context, id, symbol string, orderType, side string, amount, price float64, params map[string]interface{}) (*Order, error) {
	return nil, NewNotSupported("EditOrder")
}

func (b *BaseExchange) CancelOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*Order, error) {
	return nil, NewNotSupported("CancelOrder")
}

func (b *BaseExchange) CancelAllOrders(ctx context.Context, symbol string, params map[string]interface{}) ([]*Order, error) {
	return nil, NewNotSupported("CancelAllOrders")
}

func (b *BaseExchange) CancelOrders(ctx context.Context, ids []string, symbol string, params map[string]interface{}) ([]Order, error) {
	return nil, NewNotSupported("CancelOrders")
}

func (b *BaseExchange) FetchOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*Order, error) {
	return nil, NewNotSupported("FetchOrder")
}

func (b *BaseExchange) FetchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Order, error) {
	return nil, NewNotSupported("FetchOrders")
}

func (b *BaseExchange) FetchOpenOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Order, error) {
	return nil, NewNotSupported("FetchOpenOrders")
}

func (b *BaseExchange) FetchClosedOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Order, error) {
	return nil, NewNotSupported("FetchClosedOrders")
}

func (b *BaseExchange) FetchMyTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Trade, error) {
	return nil, NewNotSupported("FetchMyTrades")
}

// 资金管理方法的默认实现
func (b *BaseExchange) FetchDepositAddress(ctx context.Context, code string, params map[string]interface{}) (*DepositAddress, error) {
	return nil, NewNotSupported("FetchDepositAddress")
}

func (b *BaseExchange) FetchDepositAddresses(ctx context.Context, codes []string, params map[string]interface{}) (map[string]*DepositAddress, error) {
	return nil, NewNotSupported("FetchDepositAddresses")
}

func (b *BaseExchange) FetchDepositAddressesByNetwork(ctx context.Context, code string, params map[string]interface{}) (map[string]*DepositAddress, error) {
	return nil, NewNotSupported("FetchDepositAddressesByNetwork")
}

func (b *BaseExchange) CreateDepositAddress(ctx context.Context, code string, params map[string]interface{}) (*DepositAddress, error) {
	return nil, NewNotSupported("CreateDepositAddress")
}

func (b *BaseExchange) FetchDeposits(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]Transaction, error) {
	return nil, NewNotSupported("FetchDeposits")
}

func (b *BaseExchange) FetchWithdrawals(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]Transaction, error) {
	return nil, NewNotSupported("FetchWithdrawals")
}

func (b *BaseExchange) FetchTransactions(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]Transaction, error) {
	return nil, NewNotSupported("FetchTransactions")
}

func (b *BaseExchange) Withdraw(ctx context.Context, code string, amount float64, address string, tag string, params map[string]interface{}) (*Transaction, error) {
	return nil, NewNotSupported("Withdraw")
}

// 期货和保证金方法的默认实现
func (b *BaseExchange) FetchPositions(ctx context.Context, symbols []string, params map[string]interface{}) ([]*Position, error) {
	return nil, NewNotSupported("FetchPositions")
}

func (b *BaseExchange) FetchPosition(ctx context.Context, symbol string, params map[string]interface{}) (*Position, error) {
	return nil, NewNotSupported("FetchPosition")
}

func (b *BaseExchange) FetchLeverage(ctx context.Context, symbol string, params map[string]interface{}) (*Leverage, error) {
	return nil, NewNotSupported("FetchLeverage")
}

func (b *BaseExchange) SetLeverage(ctx context.Context, leverage int, symbol string, params map[string]interface{}) (*LeverageInfo, error) {
	return nil, NewNotSupported("SetLeverage")
}

func (b *BaseExchange) FetchMarginMode(ctx context.Context, symbol string, params map[string]interface{}) (*MarginMode, error) {
	return nil, NewNotSupported("FetchMarginMode")
}

func (b *BaseExchange) SetMarginMode(ctx context.Context, marginMode, symbol string, params map[string]interface{}) (*MarginMode, error) {
	return nil, NewNotSupported("SetMarginMode")
}

func (b *BaseExchange) FetchFundingRate(ctx context.Context, symbol string, params map[string]interface{}) (*FundingRate, error) {
	return nil, NewNotSupported("FetchFundingRate")
}

func (b *BaseExchange) FetchFundingRates(ctx context.Context, symbols []string, params map[string]interface{}) (map[string]*FundingRate, error) {
	return nil, NewNotSupported("FetchFundingRates")
}

func (b *BaseExchange) FetchFundingHistory(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]FundingRate, error) {
	return nil, NewNotSupported("FetchFundingHistory")
}

// WebSocket方法的默认实现
func (b *BaseExchange) WatchTicker(ctx context.Context, symbol string, params map[string]interface{}) (<-chan *WatchTicker, error) {
	return nil, NewNotSupported("WatchTicker")
}

func (b *BaseExchange) WatchTickers(ctx context.Context, symbols []string, params map[string]interface{}) (<-chan map[string]*WatchTicker, error) {
	// 默认实现：基于WatchTicker聚合多个ticker
	if len(symbols) == 0 {
		return nil, NewInvalidRequest("symbols array cannot be empty")
	}

	// 创建结果channel
	resultChan := make(chan map[string]*WatchTicker, 1000)

	// 存储每个symbol的最新ticker
	tickerMap := make(map[string]*WatchTicker)
	var tickerMutex sync.RWMutex

	// 为每个symbol启动单独的WatchTicker
	var wg sync.WaitGroup
	errorChan := make(chan error, len(symbols))

	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()

			// 观察单个ticker
			tickerChan, err := b.WatchTicker(ctx, sym, params)
			if err != nil {
				errorChan <- fmt.Errorf("failed to watch ticker for %s: %w", sym, err)
				return
			}

			// 监听ticker更新
			for ticker := range tickerChan {
				tickerMutex.Lock()
				tickerMap[sym] = ticker

				// 创建当前所有ticker的副本
				currentTickers := make(map[string]*WatchTicker)
				for k, v := range tickerMap {
					currentTickers[k] = v
				}
				tickerMutex.Unlock()

				// 发送更新
				select {
				case resultChan <- currentTickers:
				case <-ctx.Done():
					return
				}
			}
		}(symbol)
	}

	// 检查启动错误
	go func() {
		wg.Wait()
		close(errorChan)
		close(resultChan)
	}()

	// 检查是否有启动错误
	select {
	case err := <-errorChan:
		if err != nil {
			return nil, err
		}
	case <-time.After(100 * time.Millisecond):
		// 100ms后没有错误，认为启动成功
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return resultChan, nil
}

func (b *BaseExchange) WatchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (<-chan *WatchOrderBook, error) {
	return nil, NewNotSupported("WatchOrderBook")
}

func (b *BaseExchange) WatchOrderBookForSymbols(ctx context.Context, symbols []string, limit int, params map[string]interface{}) (<-chan map[string]*WatchOrderBook, error) {
	return nil, NewNotSupported("WatchOrderBookForSymbols")
}

func (b *BaseExchange) WatchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan *WatchTrade, error) {
	return nil, NewNotSupported("WatchTrades")
}

func (b *BaseExchange) WatchTradesForSymbols(ctx context.Context, symbols []string, since int64, limit int, params map[string]interface{}) (<-chan map[string][]WatchTrade, error) {
	return nil, NewNotSupported("WatchTradesForSymbols")
}

func (b *BaseExchange) WatchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) (<-chan *WatchOHLCV, error) {
	return nil, NewNotSupported("WatchOHLCV")
}

func (b *BaseExchange) WatchBalance(ctx context.Context, params map[string]interface{}) (<-chan *WatchBalance, error) {
	return nil, NewNotSupported("WatchBalance")
}

func (b *BaseExchange) WatchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan *WatchOrder, error) {
	return nil, NewNotSupported("WatchOrders")
}

func (b *BaseExchange) WatchMyTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan []WatchTrade, error) {
	return nil, NewNotSupported("WatchMyTrades")
}

// 交易所特定方法的默认实现
func (b *BaseExchange) PublicAPI(ctx context.Context, method string, params map[string]interface{}) (interface{}, error) {
	return nil, NewNotSupported("PublicAPI")
}

func (b *BaseExchange) PrivateAPI(ctx context.Context, method string, params map[string]interface{}) (interface{}, error) {
	return nil, NewNotSupported("PrivateAPI")
}

// 签名方法的默认实现
func (b *BaseExchange) Sign(path, api, method string, params map[string]interface{}, headers map[string]string, body interface{}) (string, map[string]string, interface{}, error) {
	return path, headers, body, nil
}

// 错误处理方法的默认实现
func (b *BaseExchange) HandleErrors(httpCode int, httpReason, url, method, headers, body string, response interface{}, requestHeaders, requestBody string) error {
	if httpCode >= 400 {
		return NewExchangeError(fmt.Sprintf("HTTP %d: %s", httpCode, httpReason))
	}
	return nil
}

// 限流成本计算的默认实现
func (b *BaseExchange) CalculateRateLimiterCost(api, method, path string, params map[string]interface{}, config map[string]interface{}) int {
	return 1 // 默认每个请求成本为1
}

// ========== 生命周期管理 ==========

func (b *BaseExchange) Close() error {
	// 关闭HTTP客户端连接
	if b.httpClient != nil {
		// 标准http.Client没有Close方法，无需操作
	}
	return nil
}

// GetRequestCount 获取请求计数
func (b *BaseExchange) GetRequestCount() int64 {
	return b.requestCount
}

// GetLastRequestTime 获取最后请求时间
func (b *BaseExchange) GetLastRequestTime() int64 {
	return b.lastRequestTime
}

// ========== 配置更新方法 ==========

// SetCredentials 设置API凭证
func (b *BaseExchange) SetCredentials(apiKey, secret, password, uid string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.apiKey = apiKey
	b.secret = secret
	b.password = password
	b.uid = uid
}

// SetEnvironment 设置环境配置
func (b *BaseExchange) SetEnvironment(sandbox, testnet bool, timeout int, enableRateLimit bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.sandbox = sandbox
	b.testnet = testnet
	b.timeout = time.Duration(timeout) * time.Second
	b.enableRateLimit = enableRateLimit
}

// SetNetworking 设置网络配置
func (b *BaseExchange) SetNetworking(userAgent, proxy string, headers map[string]string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if userAgent != "" {
		b.userAgent = userAgent
	}
	if proxy != "" {
		b.httpProxy = proxy
	}
	if headers != nil {
		b.headers = headers
	}
}

// SetCapabilities 设置功能支持
func (b *BaseExchange) SetCapabilities(capabilities map[string]bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.has = capabilities
}

// SetTimeframes 设置时间周期
func (b *BaseExchange) SetTimeframes(timeframes map[string]string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.timeframes = timeframes
}

// SetFees 设置费率信息
func (b *BaseExchange) SetFees(fees map[string]map[string]interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.fees = fees
}

// SetMarkets 设置市场信息
func (b *BaseExchange) SetMarkets(markets map[string]*Market) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.markets = markets
	// 同时更新索引
	b.marketsById = make(map[string]*Market)
	for _, market := range markets {
		b.marketsById[market.ID] = market
	}
}

// GetMarkets 获取市场信息
func (b *BaseExchange) GetMarkets() map[string]*Market {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.markets
}

// ParseBalance 解析余额数据
func (b *BaseExchange) ParseBalance(response map[string]interface{}) (*Account, error) {
	return nil, NewNotSupported("ParseBalance")
}

// ParseOHLCV 解析OHLCV数据
func (b *BaseExchange) ParseOHLCV(ohlcv []interface{}, market *Market, timeframe string) (*OHLCV, error) {
	return nil, NewNotSupported("ParseOHLCV")
}

// ParseTicker 解析Ticker数据
func (b *BaseExchange) ParseTicker(ticker map[string]interface{}, market *Market) (*Ticker, error) {
	return nil, NewNotSupported("ParseTicker")
}

// ParseTrade 解析Trade数据
func (b *BaseExchange) ParseTrade(trade map[string]interface{}, market *Market) (*Trade, error) {
	return nil, NewNotSupported("ParseTrade")
}

// ParseOrder 解析Order数据
func (b *BaseExchange) ParseOrder(order map[string]interface{}, market *Market) (*Order, error) {
	return nil, NewNotSupported("ParseOrder")
}
