package ccxt

import (
	"context"
	"time"
)

// ========== 核心交易所接口 ==========

// Exchange 统一交易所接口
type Exchange interface {
	GetID() string
	GetName() string
	GetCountries() []string
	GetVersion() string
	GetRateLimit() int
	GetSandbox() bool
	Has() map[string]bool
	GetTimeframes() map[string]string

	LoadMarkets(ctx context.Context, reload ...bool) (map[string]*Market, error)
	Close() error

	FetchMarkets(ctx context.Context, params map[string]interface{}) ([]*Market, error)
	FetchCurrencies(ctx context.Context, params map[string]interface{}) (map[string]*Currency, error)
	FetchTicker(ctx context.Context, symbol string, params map[string]interface{}) (*Ticker, error)
	FetchTickers(ctx context.Context, symbols []string, params map[string]interface{}) (map[string]*Ticker, error)
	FetchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) ([]*OHLCV, error)
	FetchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Trade, error)
	FetchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (*OrderBook, error)

	FetchBalance(ctx context.Context, params map[string]interface{}) (*Account, error)
	FetchMyTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Trade, error)
	FetchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Order, error)
	FetchOpenOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Order, error)
	FetchClosedOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*Order, error)

	CreateOrder(ctx context.Context, symbol, orderType, side string, amount, price float64, params map[string]interface{}) (*Order, error)
	CancelOrder(ctx context.Context, id string, symbol string, params map[string]interface{}) (*Order, error)
	CancelAllOrders(ctx context.Context, symbol string, params map[string]interface{}) ([]*Order, error)
	EditOrder(ctx context.Context, id string, symbol, orderType, side string, amount, price float64, params map[string]interface{}) (*Order, error)
	FetchOrder(ctx context.Context, id string, symbol string, params map[string]interface{}) (*Order, error)

	FetchPositions(ctx context.Context, symbols []string, params map[string]interface{}) ([]*Position, error)
	FetchPosition(ctx context.Context, symbol string, params map[string]interface{}) (*Position, error)
	FetchFundingRate(ctx context.Context, symbol string, params map[string]interface{}) (*FundingRate, error)
	FetchFundingRates(ctx context.Context, symbols []string, params map[string]interface{}) (map[string]*FundingRate, error)
	SetLeverage(ctx context.Context, leverage int, symbol string, params map[string]interface{}) (*LeverageInfo, error)
	SetMarginMode(ctx context.Context, marginMode, symbol string, params map[string]interface{}) (*MarginMode, error)

	WatchTicker(ctx context.Context, symbol string, params map[string]interface{}) (<-chan *WatchTicker, error)
	WatchTickers(ctx context.Context, symbols []string, params map[string]interface{}) (<-chan map[string]*WatchTicker, error)
	WatchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) (<-chan *WatchOHLCV, error)
	WatchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan *WatchTrade, error)
	WatchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (<-chan *WatchOrderBook, error)
	WatchBalance(ctx context.Context, params map[string]interface{}) (<-chan *WatchBalance, error)
	WatchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan *WatchOrder, error)

	Request(ctx context.Context, url string, method string, headers map[string]string, body interface{}, params map[string]interface{}) (*Response, error)
	ParseTicker(ticker map[string]interface{}, market *Market) (*Ticker, error)
	ParseOHLCV(ohlcv []interface{}, market *Market, timeframe string) (*OHLCV, error)
	ParseTrade(trade map[string]interface{}, market *Market) (*Trade, error)
	ParseOrder(order map[string]interface{}, market *Market) (*Order, error)
	ParseBalance(response map[string]interface{}) (*Account, error)
}

// ExchangeConfig 交易所配置接口
type ExchangeConfig interface {
	GetAPIKey() string
	GetSecret() string
	GetPassword() string
	GetUID() string
	GetSandbox() bool
	GetTestnet() bool
	GetTimeout() time.Duration
	GetRateLimit() int
	GetEnableRateLimit() bool
	GetProxy() string
	GetUserAgent() string
	GetMarketType() string
	GetHeaders() map[string]string
	GetOptions() map[string]interface{}

	SetAPIKey(string)
	SetSecret(string)
	SetPassword(string)
	SetUID(string)
	SetSandbox(bool)
	SetTestnet(bool)
	SetTimeout(time.Duration)
	SetRateLimit(int)
	SetEnableRateLimit(bool)
	SetProxy(string)
	SetUserAgent(string)
	SetHeaders(map[string]string)
	SetOptions(map[string]interface{})
}

// DefaultExchangeConfig 默认配置实现
type DefaultExchangeConfig struct {
	apiKey          string
	secret          string
	password        string
	uid             string
	sandbox         bool
	testnet         bool
	timeout         time.Duration
	rateLimit       int
	enableRateLimit bool
	proxy           string
	userAgent       string
	marketType      string
	headers         map[string]string
	options         map[string]interface{}
}

// NewDefaultExchangeConfig 创建默认配置
func NewDefaultExchangeConfig() *DefaultExchangeConfig {
	return &DefaultExchangeConfig{
		timeout:         30 * time.Second,
		rateLimit:       1000,
		enableRateLimit: true,
		userAgent:       "DataHive/1.0.0",
		marketType:      "spot",
		headers:         make(map[string]string),
		options:         make(map[string]interface{}),
	}
}

// Getter 方法
func (c *DefaultExchangeConfig) GetAPIKey() string                  { return c.apiKey }
func (c *DefaultExchangeConfig) GetSecret() string                  { return c.secret }
func (c *DefaultExchangeConfig) GetPassword() string                { return c.password }
func (c *DefaultExchangeConfig) GetUID() string                     { return c.uid }
func (c *DefaultExchangeConfig) GetSandbox() bool                   { return c.sandbox }
func (c *DefaultExchangeConfig) GetTestnet() bool                   { return c.testnet }
func (c *DefaultExchangeConfig) GetTimeout() time.Duration          { return c.timeout }
func (c *DefaultExchangeConfig) GetRateLimit() int                  { return c.rateLimit }
func (c *DefaultExchangeConfig) GetEnableRateLimit() bool           { return c.enableRateLimit }
func (c *DefaultExchangeConfig) GetProxy() string                   { return c.proxy }
func (c *DefaultExchangeConfig) GetUserAgent() string               { return c.userAgent }
func (c *DefaultExchangeConfig) GetMarketType() string              { return c.marketType }
func (c *DefaultExchangeConfig) GetHeaders() map[string]string      { return c.headers }
func (c *DefaultExchangeConfig) GetOptions() map[string]interface{} { return c.options }

// Setter 方法
func (c *DefaultExchangeConfig) SetAPIKey(v string)                  { c.apiKey = v }
func (c *DefaultExchangeConfig) SetSecret(v string)                  { c.secret = v }
func (c *DefaultExchangeConfig) SetPassword(v string)                { c.password = v }
func (c *DefaultExchangeConfig) SetUID(v string)                     { c.uid = v }
func (c *DefaultExchangeConfig) SetSandbox(v bool)                   { c.sandbox = v }
func (c *DefaultExchangeConfig) SetTestnet(v bool)                   { c.testnet = v }
func (c *DefaultExchangeConfig) SetTimeout(v time.Duration)          { c.timeout = v }
func (c *DefaultExchangeConfig) SetRateLimit(v int)                  { c.rateLimit = v }
func (c *DefaultExchangeConfig) SetEnableRateLimit(v bool)           { c.enableRateLimit = v }
func (c *DefaultExchangeConfig) SetProxy(v string)                   { c.proxy = v }
func (c *DefaultExchangeConfig) SetUserAgent(v string)               { c.userAgent = v }
func (c *DefaultExchangeConfig) SetHeaders(v map[string]string)      { c.headers = v }
func (c *DefaultExchangeConfig) SetOptions(v map[string]interface{}) { c.options = v }

// ========== 工厂接口 ==========

// ExchangeFactory 交易所工厂接口
type ExchangeFactory interface {
	CreateExchange(id string, config ExchangeConfig) (Exchange, error)
	GetSupportedExchanges() []string
	IsSupported(id string) bool
	RegisterExchange(id string, creator ExchangeCreator)
}

// ExchangeCreator 交易所创建函数类型
type ExchangeCreator func(config ExchangeConfig) (Exchange, error)

// ========== 响应结构 ==========

// Response HTTP响应
type Response struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
	URL        string
}

// FetchOHLCVOptions OHLCV获取选项
type FetchOHLCVOptions struct {
	Symbol    string
	Timeframe string
	Since     int64
	Limit     int
	Params    map[string]interface{}
}

// WithFetchOHLCVSymbol 设置交易对
func WithFetchOHLCVSymbol(symbol string) func(*FetchOHLCVOptions) {
	return func(o *FetchOHLCVOptions) {
		o.Symbol = symbol
	}
}

// WithFetchOHLCVTimeframe 设置时间周期
func WithFetchOHLCVTimeframe(timeframe string) func(*FetchOHLCVOptions) {
	return func(o *FetchOHLCVOptions) {
		o.Timeframe = timeframe
	}
}

// WithFetchOHLCVSince 设置开始时间
func WithFetchOHLCVSince(since int64) func(*FetchOHLCVOptions) {
	return func(o *FetchOHLCVOptions) {
		o.Since = since
	}
}

// WithFetchOHLCVLimit 设置数量限制
func WithFetchOHLCVLimit(limit int) func(*FetchOHLCVOptions) {
	return func(o *FetchOHLCVOptions) {
		o.Limit = limit
	}
}

// WithFetchOHLCVParams 设置额外参数
func WithFetchOHLCVParams(params map[string]interface{}) func(*FetchOHLCVOptions) {
	return func(o *FetchOHLCVOptions) {
		o.Params = params
	}
}

// CreateOrderOptions 创建订单选项
type CreateOrderOptions struct {
	Symbol string
	Type   string
	Side   string
	Amount float64
	Price  float64
	Params map[string]interface{}
}

// WithCreateOrderPrice 设置价格
func WithCreateOrderPrice(price float64) func(*CreateOrderOptions) {
	return func(o *CreateOrderOptions) {
		o.Price = price
	}
}

// WithCreateOrderParams 设置额外参数
func WithCreateOrderParams(params map[string]interface{}) func(*CreateOrderOptions) {
	return func(o *CreateOrderOptions) {
		o.Params = params
	}
}
