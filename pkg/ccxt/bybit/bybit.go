package bybit

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"datahive/pkg/ccxt"
)

// 注册Bybit交易所
func init() {
	ccxt.RegisterExchange("bybit", func(config ccxt.ExchangeConfig) (ccxt.Exchange, error) {
		// 将通用配置转换为Bybit内部配置
		bybitConfig := &Config{
			APIKey:          config.GetAPIKey(),
			Secret:          config.GetSecret(),
			Sandbox:         config.GetSandbox(),
			TestNet:         config.GetTestnet(),
			Timeout:         int(config.GetTimeout() / time.Millisecond),
			EnableRateLimit: config.GetEnableRateLimit(),
			Proxy:           config.GetProxy(),
			UserAgent:       config.GetUserAgent(),
			Headers:         config.GetHeaders(),
			Options:         config.GetOptions(),
			DefaultType:     config.GetMarketType(),
			WSMaxReconnect:  5,    // 默认值
			RecvWindow:      5000, // 默认值
		}

		// 从BaseConfig获取DefaultType和EnableWebSocket
		if baseConfig, ok := config.(*ccxt.BaseConfig); ok {
			bybitConfig.DefaultType = baseConfig.DefaultType
			bybitConfig.EnableWebSocket = baseConfig.EnableWebSocket
			bybitConfig.WSMaxReconnect = baseConfig.WSMaxReconnect
			bybitConfig.RecvWindow = baseConfig.RecvWindow
		} else {
			// 非BaseConfig的兜底默认值
			bybitConfig.DefaultType = "linear"
			bybitConfig.EnableWebSocket = false
		}

		return New(bybitConfig)
	})
}

// Bybit Bybit 交易所实现
type Bybit struct {
	*ccxt.BaseExchange
	config        *Config
	endpoints     map[string]string
	subscriptions map[string]bool
}

// New 创建新的 Bybit 交易所实例
func New(config *Config) (*Bybit, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	base := ccxt.NewBaseExchange("bybit", "Bybit", "v5", []string{"VG", "SG"})
	bybit := &Bybit{
		BaseExchange:  base,
		config:        config.Clone(),
		endpoints:     make(map[string]string),
		subscriptions: make(map[string]bool),
	}

	// 设置基础信息
	bybit.SetBasicInfo()

	// 设置支持的功能
	bybit.SetCapabilities()

	// 设置时间周期
	bybit.SetTimeframes()

	// 设置API端点
	bybit.SetEndpoints()

	// 设置费率信息
	bybit.SetFees()

	return bybit, nil
}

// ========== 基础设置方法 ==========

// SetBasicInfo 设置基础信息
func (b *Bybit) SetBasicInfo() {
	// 设置API凭证和环境配置
	b.BaseExchange.SetCredentials(b.config.APIKey, b.config.Secret, "", "")
	b.BaseExchange.SetEnvironment(b.config.Sandbox, b.config.TestNet, b.config.Timeout, b.config.EnableRateLimit)

	// 设置网络配置
	headers := make(map[string]string)
	for k, v := range b.config.Headers {
		headers[k] = v
	}
	b.BaseExchange.SetNetworking(b.config.UserAgent, b.config.Proxy, headers)
}

// SetCapabilities 设置支持的功能
func (b *Bybit) SetCapabilities() {
	capabilities := map[string]bool{
		"CORS":                      false,
		"spot":                      true,
		"margin":                    false, // Bybit 主要是合约，现货保证金支持有限
		"future":                    true,
		"option":                    true,
		"addMargin":                 true,
		"cancelAllOrders":           true,
		"cancelOrder":               true,
		"cancelOrders":              true,
		"createDepositAddress":      true,
		"createOrder":               true,
		"createStopLimitOrder":      true,
		"createStopMarketOrder":     true,
		"createStopOrder":           true,
		"editOrder":                 true,
		"fetchBalance":              true,
		"fetchBorrowRate":           false,
		"fetchBorrowRateHistories":  false,
		"fetchBorrowRateHistory":    false,
		"fetchBorrowRates":          false,
		"fetchBorrowRatesPerSymbol": false,
		"fetchCurrencies":           true,
		"fetchDepositAddress":       true,
		"fetchDeposits":             true,
		"fetchFundingHistory":       true,
		"fetchFundingRate":          true,
		"fetchFundingRateHistory":   true,
		"fetchFundingRates":         true,
		"fetchIndexOHLCV":           false,
		"fetchLeverage":             true,
		"fetchLeverageTiers":        true,
		"fetchMarginMode":           true,
		"fetchMarkets":              true,
		"fetchMarkOHLCV":            false,
		"fetchMyTrades":             true,
		"fetchOHLCV":                true,
		"fetchOpenInterest":         true,
		"fetchOpenInterestHistory":  true,
		"fetchOpenOrders":           true,
		"fetchOrder":                true,
		"fetchOrderBook":            true,
		"fetchOrders":               true,
		"fetchOrderTrades":          true,
		"fetchPositions":            true,
		"fetchPremiumIndexOHLCV":    false,
		"fetchTicker":               true,
		"fetchTickers":              true,
		"fetchTime":                 true,
		"fetchTrades":               true,
		"fetchTradingFee":           true,
		"fetchTradingFees":          true,
		"fetchWithdrawals":          true,
		"reduceMargin":              true,
		"setLeverage":               true,
		"setMarginMode":             true,
		"setPositionMode":           true,
		"transfer":                  true,
		"withdraw":                  true,
		"ws":                        true,
	}

	b.BaseExchange.SetCapabilities(capabilities)
}

// SetTimeframes 设置时间周期
func (b *Bybit) SetTimeframes() {
	timeframes := map[string]string{
		"1m":  "1",
		"3m":  "3",
		"5m":  "5",
		"15m": "15",
		"30m": "30",
		"1h":  "60",
		"2h":  "120",
		"4h":  "240",
		"6h":  "360",
		"12h": "720",
		"1d":  "D",
		"1w":  "W",
		"1M":  "M",
	}

	b.BaseExchange.SetTimeframes(timeframes)
}

// SetEndpoints 设置API端点
func (b *Bybit) SetEndpoints() {
	baseURL := b.config.GetBaseURL()

	b.endpoints = map[string]string{
		// 公共接口
		"time":        baseURL + "/v5/market/time",
		"instruments": baseURL + "/v5/market/instruments-info",
		"ticker":      baseURL + "/v5/market/tickers",
		"orderbook":   baseURL + "/v5/market/orderbook",
		"klines":      baseURL + "/v5/market/kline",
		"trades":      baseURL + "/v5/market/recent-trade",
		"funding":     baseURL + "/v5/market/funding/history",

		// 私有接口
		"balance":     baseURL + "/v5/account/wallet-balance",
		"positions":   baseURL + "/v5/position/list",
		"order":       baseURL + "/v5/order/create",
		"cancelOrder": baseURL + "/v5/order/cancel",
		"getOrder":    baseURL + "/v5/order/realtime",
		"getOrders":   baseURL + "/v5/order/history",
		"myTrades":    baseURL + "/v5/execution/list",
		"leverage":    baseURL + "/v5/position/set-leverage",
		"marginMode":  baseURL + "/v5/account/set-margin-mode",

		// 私有接口 - 资金
		"depositAddress":  baseURL + "/v5/asset/deposit/query-address",
		"depositHistory":  baseURL + "/v5/asset/deposit/query-record",
		"withdrawHistory": baseURL + "/v5/asset/withdraw/query-record",
		"withdraw":        baseURL + "/v5/asset/withdraw/create",
	}
}

// SetFees 设置费率信息
func (b *Bybit) SetFees() {
	fees := map[string]map[string]interface{}{
		"trading": {
			"linearPerpetual": map[string]interface{}{
				"maker": 0.0001, // 0.01%
				"taker": 0.0006, // 0.06%
			},
			"inversePerpetual": map[string]interface{}{
				"maker": -0.00025, // -0.025%
				"taker": 0.00075,  // 0.075%
			},
			"spot": map[string]interface{}{
				"maker": 0.001, // 0.1%
				"taker": 0.001, // 0.1%
			},
			"option": map[string]interface{}{
				"maker": 0.0003, // 0.03%
				"taker": 0.0003, // 0.03%
			},
		},
		"funding": map[string]interface{}{
			"withdraw": map[string]interface{}{
				// 提现费用因币种而异，需要从API获取
			},
			"deposit": 0.0, // 充值免费
		},
	}

	b.BaseExchange.SetFees(fees)
}

// ========== 市场数据方法 ==========

// LoadMarkets 加载市场信息
func (b *Bybit) LoadMarkets(ctx context.Context, reload ...bool) (map[string]*ccxt.Market, error) {
	// 处理可变参数
	shouldReload := false
	if len(reload) > 0 {
		shouldReload = reload[0]
	}

	if !shouldReload && len(b.BaseExchange.GetMarkets()) > 0 {
		return b.BaseExchange.GetMarkets(), nil
	}

	// 获取不同类型的市场信息
	markets := make(map[string]*ccxt.Market)

	// 获取现货市场
	if spotMarkets, err := b.fetchMarketsByCategory(ctx, "spot"); err == nil {
		for symbol, market := range spotMarkets {
			markets[symbol] = market
		}
	}

	// 获取线性合约市场
	if linearMarkets, err := b.fetchMarketsByCategory(ctx, "linear"); err == nil {
		for symbol, market := range linearMarkets {
			markets[symbol] = market
		}
	}

	// 获取反向合约市场
	if inverseMarkets, err := b.fetchMarketsByCategory(ctx, "inverse"); err == nil {
		for symbol, market := range inverseMarkets {
			markets[symbol] = market
		}
	}

	// 获取期权市场
	if optionMarkets, err := b.fetchMarketsByCategory(ctx, "option"); err == nil {
		for symbol, market := range optionMarkets {
			markets[symbol] = market
		}
	}

	b.BaseExchange.SetMarkets(markets)
	return markets, nil
}

// fetchMarketsByCategory 根据类别获取市场信息
func (b *Bybit) fetchMarketsByCategory(ctx context.Context, category string) (map[string]*ccxt.Market, error) {
	params := map[string]interface{}{
		"category": category,
	}

	response, err := b.PublicAPI(ctx, "instruments", params)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	markets := make(map[string]*ccxt.Market)
	for _, item := range listData {
		instrumentData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		market := b.parseMarket(instrumentData)
		if market != nil {
			markets[market.Symbol] = market
		}
	}

	return markets, nil
}

// FetchTicker 获取ticker
func (b *Bybit) FetchTicker(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.Ticker, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category": b.getMarketCategory(market.Type),
		"symbol":   market.ID,
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PublicAPI(ctx, "ticker", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok || len(listData) == 0 {
		return nil, fmt.Errorf("no ticker data found")
	}

	tickerData, ok := listData[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid ticker data format")
	}

	return b.parseTicker(tickerData, market), nil
}

// FetchOHLCV 获取K线数据
func (b *Bybit) FetchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) ([]*ccxt.OHLCV, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	interval, ok := b.BaseExchange.GetTimeframes()[timeframe]
	if !ok {
		return nil, ccxt.NewBadRequest(fmt.Sprintf("unsupported timeframe %s", timeframe))
	}

	reqParams := map[string]interface{}{
		"category": b.getMarketCategory(market.Type),
		"symbol":   market.ID,
		"interval": interval,
	}

	if limit > 0 {
		if limit > 1000 {
			limit = 1000 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	if since > 0 {
		reqParams["start"] = since
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PublicAPI(ctx, "klines", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	ohlcvs := make([]*ccxt.OHLCV, 0, len(listData))
	for _, item := range listData {
		klineData, ok := item.([]interface{})
		if !ok || len(klineData) < 7 {
			continue
		}

		ohlcv := b.parseKline(klineData, market)
		if ohlcv != nil {
			ohlcvs = append(ohlcvs, ohlcv)
		}
	}

	return ohlcvs, nil
}

// FetchTrades 获取交易记录
func (b *Bybit) FetchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Trade, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category": b.getMarketCategory(market.Type),
		"symbol":   market.ID,
	}

	if limit > 0 {
		if limit > 1000 {
			limit = 1000 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PublicAPI(ctx, "trades", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	trades := make([]*ccxt.Trade, 0, len(listData))
	for _, item := range listData {
		tradeData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		trade := b.parseTrade(tradeData, market)
		if trade != nil {
			trades = append(trades, trade)
		}
	}

	return trades, nil
}

// FetchOrderBook 获取订单簿
func (b *Bybit) FetchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (*ccxt.OrderBook, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category": b.getMarketCategory(market.Type),
		"symbol":   market.ID,
	}

	if limit > 0 {
		if limit > 500 {
			limit = 500 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PublicAPI(ctx, "orderbook", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return b.parseOrderBook(result, market), nil
}

// ========== 账户方法 ==========

// FetchBalance 获取账户余额
func (b *Bybit) FetchBalance(ctx context.Context, params map[string]interface{}) (*ccxt.Account, error) {
	reqParams := map[string]interface{}{
		"accountType": b.getAccountType(),
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "balance", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok || len(listData) == 0 {
		return nil, fmt.Errorf("no balance data found")
	}

	balanceData, ok := listData[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid balance data format")
	}

	return b.parseBalance(balanceData), nil
}

// ========== 交易方法 ==========

// CreateOrder 创建订单
func (b *Bybit) CreateOrder(ctx context.Context, symbol, orderType, side string, amount, price float64, params map[string]interface{}) (*ccxt.Order, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category":    b.getMarketCategory(market.Type),
		"symbol":      market.ID,
		"side":        strings.Title(side),
		"orderType":   b.mapOrderType(orderType),
		"qty":         b.formatAmount(amount, market),
		"timeInForce": "GTC", // 默认 Good Till Canceled
	}

	if orderType == ccxt.OrderTypeLimit {
		reqParams["price"] = b.formatPrice(price, market)
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "POST", "order", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return b.parseOrder(result, market), nil
}

// ========== 完整的交易方法 ==========

// CancelOrder 取消订单
func (b *Bybit) CancelOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*ccxt.Order, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category": b.getMarketCategory(market.Type),
		"symbol":   market.ID,
		"orderId":  id,
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "POST", "cancelOrder", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return b.parseOrder(result, market), nil
}

// FetchOrder 获取订单详情
func (b *Bybit) FetchOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*ccxt.Order, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category": b.getMarketCategory(market.Type),
		"orderId":  id,
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "getOrder", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok || len(listData) == 0 {
		return nil, ccxt.NewOrderNotFound(fmt.Sprintf("order %s not found", id))
	}

	orderData, ok := listData[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid order data format")
	}

	return b.parseOrder(orderData, market), nil
}

// FetchOrders 获取订单历史
func (b *Bybit) FetchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Order, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	reqParams := map[string]interface{}{
		"category": b.config.Category,
	}

	if symbol != "" {
		market := b.getMarket(symbol)
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		reqParams["symbol"] = market.ID
	}

	if limit > 0 {
		if limit > 50 {
			limit = 50 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "getOrders", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	orders := make([]*ccxt.Order, 0, len(listData))
	for _, item := range listData {
		orderData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		var market *ccxt.Market
		if symbol != "" {
			market = b.getMarket(symbol)
		}

		order := b.parseOrder(orderData, market)
		if order != nil {
			orders = append(orders, order)
		}
	}

	return orders, nil
}

// FetchOpenOrders 获取开放订单
func (b *Bybit) FetchOpenOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Order, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	reqParams := map[string]interface{}{
		"category": b.config.Category,
	}

	if symbol != "" {
		market := b.getMarket(symbol)
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		reqParams["symbol"] = market.ID
	}

	if limit > 0 {
		if limit > 50 {
			limit = 50 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "getOrder", reqParams) // 实时订单查询
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	orders := make([]*ccxt.Order, 0, len(listData))
	for _, item := range listData {
		orderData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		var market *ccxt.Market
		if symbol != "" {
			market = b.getMarket(symbol)
		}

		order := b.parseOrder(orderData, market)
		if order != nil && order.Status == "open" {
			orders = append(orders, order)
		}
	}

	return orders, nil
}

// FetchMyTrades 获取我的交易记录
func (b *Bybit) FetchMyTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Trade, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	reqParams := map[string]interface{}{
		"category": b.config.Category,
	}

	if symbol != "" {
		market := b.getMarket(symbol)
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		reqParams["symbol"] = market.ID
	}

	if limit > 0 {
		if limit > 100 {
			limit = 100 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "myTrades", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	trades := make([]*ccxt.Trade, 0, len(listData))
	for _, item := range listData {
		tradeData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		trade := b.parseMyTrade(tradeData, symbol)
		if trade != nil {
			trades = append(trades, trade)
		}
	}

	return trades, nil
}

// ========== 杠杆和保证金管理 ==========

// SetLeverage 设置杠杆倍数
func (b *Bybit) SetLeverage(ctx context.Context, leverage int, symbol string, params map[string]interface{}) (*ccxt.LeverageInfo, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market := b.getMarket(symbol)
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"category":     b.getMarketCategory(market.Type),
		"symbol":       market.ID,
		"buyLeverage":  fmt.Sprintf("%.2f", float64(leverage)),
		"sellLeverage": fmt.Sprintf("%.2f", float64(leverage)),
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "POST", "leverage", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return &ccxt.LeverageInfo{
		Leverage: leverage,
		Symbol:   symbol,
		Info:     result,
	}, nil
}

// FetchLeverage 获取杠杆信息
func (b *Bybit) FetchLeverage(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.LeverageInfo, error) {
	positions, err := b.FetchPositions(ctx, []string{symbol}, params)
	if err != nil {
		return nil, err
	}

	if len(positions) == 0 {
		return &ccxt.LeverageInfo{
			Leverage: 1.0,
			Symbol:   symbol,
			Info:     map[string]interface{}{},
		}, nil
	}

	return &ccxt.LeverageInfo{
		Leverage: int(positions[0].Leverage),
		Symbol:   symbol,
		Info:     positions[0].Info,
	}, nil
}

// SetMarginMode 设置保证金模式
func (b *Bybit) SetMarginMode(ctx context.Context, marginMode, symbol string, params map[string]interface{}) (*ccxt.MarginModeInfo, error) {
	reqParams := map[string]interface{}{
		"tradeMode": b.convertMarginMode(marginMode),
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "POST", "marginMode", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return &ccxt.MarginModeInfo{
		MarginMode: marginMode,
		Symbol:     symbol,
		Info:       result,
	}, nil
}

// FetchMarginMode 获取保证金模式
func (b *Bybit) FetchMarginMode(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.MarginModeInfo, error) {
	positions, err := b.FetchPositions(ctx, []string{symbol}, params)
	if err != nil {
		return nil, err
	}

	marginMode := "isolated" // 默认值
	var info map[string]interface{}

	if len(positions) > 0 {
		// 从仓位信息中获取保证金模式
		if tradeMode, ok := positions[0].Info["tradeMode"].(string); ok {
			marginMode = b.parseMarginMode(tradeMode)
		}
		info = positions[0].Info
	}

	return &ccxt.MarginModeInfo{
		MarginMode: marginMode,
		Symbol:     symbol,
		Info:       info,
	}, nil
}

// ========== 高级订单类型 ==========

// CreateStopLossOrder 创建止损订单
func (b *Bybit) CreateStopLossOrder(ctx context.Context, symbol, side string, amount, price, stopPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}

	orderParams["stopLoss"] = fmt.Sprintf("%.8f", stopPrice)
	orderParams["orderType"] = "Market" // 止损通常使用市价单

	return b.CreateOrder(ctx, symbol, "market", side, amount, price, orderParams)
}

// CreateStopLossLimitOrder 创建止损限价订单
func (b *Bybit) CreateStopLossLimitOrder(ctx context.Context, symbol, side string, amount, price, stopPrice, limitPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}

	orderParams["stopLoss"] = fmt.Sprintf("%.8f", stopPrice)
	orderParams["orderType"] = "Limit"

	return b.CreateOrder(ctx, symbol, "limit", side, amount, limitPrice, orderParams)
}

// CreateTakeProfitOrder 创建止盈订单
func (b *Bybit) CreateTakeProfitOrder(ctx context.Context, symbol, side string, amount, price, takeProfitPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}

	orderParams["takeProfit"] = fmt.Sprintf("%.8f", takeProfitPrice)
	orderParams["orderType"] = "Market"

	return b.CreateOrder(ctx, symbol, "market", side, amount, price, orderParams)
}

// CreateTakeProfitLimitOrder 创建止盈限价订单
func (b *Bybit) CreateTakeProfitLimitOrder(ctx context.Context, symbol, side string, amount, price, takeProfitPrice, limitPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}

	orderParams["takeProfit"] = fmt.Sprintf("%.8f", takeProfitPrice)
	orderParams["orderType"] = "Limit"

	return b.CreateOrder(ctx, symbol, "limit", side, amount, limitPrice, orderParams)
}

// ========== 资金管理 ==========

// FetchDepositAddress 获取充值地址
func (b *Bybit) FetchDepositAddress(ctx context.Context, code string, params map[string]interface{}) (*ccxt.DepositAddress, error) {
	reqParams := map[string]interface{}{
		"coin": code,
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "depositAddress", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return b.parseDepositAddress(result, code), nil
}

// FetchDepositHistory 获取充值历史
func (b *Bybit) FetchDepositHistory(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Transaction, error) {
	reqParams := map[string]interface{}{}

	if code != "" {
		reqParams["coin"] = code
	}

	if limit > 0 {
		if limit > 50 {
			limit = 50 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "depositHistory", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["rows"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	deposits := make([]*ccxt.Transaction, 0, len(listData))
	for _, item := range listData {
		depositData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		deposit := b.parseDeposit(depositData)
		if deposit != nil {
			deposits = append(deposits, deposit)
		}
	}

	return deposits, nil
}

// FetchWithdrawHistory 获取提现历史
func (b *Bybit) FetchWithdrawHistory(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Transaction, error) {
	reqParams := map[string]interface{}{}

	if code != "" {
		reqParams["coin"] = code
	}

	if limit > 0 {
		if limit > 50 {
			limit = 50 // Bybit 限制
		}
		reqParams["limit"] = limit
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "withdrawHistory", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["rows"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	withdrawals := make([]*ccxt.Transaction, 0, len(listData))
	for _, item := range listData {
		withdrawData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		withdrawal := b.parseWithdraw(withdrawData)
		if withdrawal != nil {
			withdrawals = append(withdrawals, withdrawal)
		}
	}

	return withdrawals, nil
}

// Withdraw 提现
func (b *Bybit) Withdraw(ctx context.Context, code string, amount float64, address, tag string, params map[string]interface{}) (*ccxt.Transaction, error) {
	reqParams := map[string]interface{}{
		"coin":    code,
		"chain":   b.getDefaultChain(code),
		"address": address,
		"amount":  fmt.Sprintf("%.8f", amount),
	}

	if tag != "" {
		reqParams["tag"] = tag
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "POST", "withdraw", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	return b.parseWithdraw(result), nil
}

// ========== 精度处理方法 ==========

// AmountToPrecision 将数量格式化为交易对精度
func (b *Bybit) AmountToPrecision(symbol string, amount float64) float64 {
	market := b.getMarket(symbol)
	if market == nil {
		return amount
	}
	return b.floatToPrecision(amount, market.Precision.Amount)
}

// PriceToPrecision 将价格格式化为交易对精度
func (b *Bybit) PriceToPrecision(symbol string, price float64) float64 {
	market := b.getMarket(symbol)
	if market == nil {
		return price
	}
	return b.floatToPrecision(price, market.Precision.Price)
}

// CostToPrecision 将成本格式化为交易对精度
func (b *Bybit) CostToPrecision(symbol string, cost float64) float64 {
	market := b.getMarket(symbol)
	if market == nil {
		return cost
	}
	return b.floatToPrecision(cost, market.Precision.Price)
}

// ValidateMarketLimits 验证订单是否符合市场限制
func (b *Bybit) ValidateMarketLimits(symbol string, amount, price, cost float64) error {
	market := b.getMarket(symbol)
	if market == nil {
		return ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	// 检查最小/最大数量
	if market.Limits.Amount.Min > 0 && amount < market.Limits.Amount.Min {
		return ccxt.NewInvalidOrder(fmt.Sprintf("amount %.8f is below minimum %.8f", amount, market.Limits.Amount.Min), "")
	}
	if market.Limits.Amount.Max > 0 && amount > market.Limits.Amount.Max {
		return ccxt.NewInvalidOrder(fmt.Sprintf("amount %.8f exceeds maximum %.8f", amount, market.Limits.Amount.Max), "")
	}

	// 检查最小/最大价格
	if price > 0 {
		if market.Limits.Price.Min > 0 && price < market.Limits.Price.Min {
			return ccxt.NewInvalidOrder(fmt.Sprintf("price %.8f is below minimum %.8f", price, market.Limits.Price.Min), "")
		}
		if market.Limits.Price.Max > 0 && price > market.Limits.Price.Max {
			return ccxt.NewInvalidOrder(fmt.Sprintf("price %.8f exceeds maximum %.8f", price, market.Limits.Price.Max), "")
		}
	}

	// 检查最小/最大成本
	if cost > 0 {
		if market.Limits.Cost.Min > 0 && cost < market.Limits.Cost.Min {
			return ccxt.NewInvalidOrder(fmt.Sprintf("cost %.8f is below minimum %.8f", cost, market.Limits.Cost.Min), "")
		}
		if market.Limits.Cost.Max > 0 && cost > market.Limits.Cost.Max {
			return ccxt.NewInvalidOrder(fmt.Sprintf("cost %.8f exceeds maximum %.8f", cost, market.Limits.Cost.Max), "")
		}
	}

	return nil
}

// ========== 错误处理增强 ==========

// HandleErrors 处理API错误响应
func (b *Bybit) HandleErrors(response map[string]interface{}, body string) error {
	// 检查返回码
	if retCode, ok := response["retCode"].(float64); ok && retCode != 0 {
		retMsg := b.SafeString(response, "retMsg", "Unknown error")
		return b.mapBybitError(int(retCode), retMsg, body)
	}

	// 检查旧版本的错误格式
	if retCode, ok := response["ret_code"].(float64); ok && retCode != 0 {
		retMsg := b.SafeString(response, "ret_msg", "Unknown error")
		return b.mapBybitError(int(retCode), retMsg, body)
	}

	return nil
}

// mapBybitError 映射Bybit错误代码到CCXT错误类型
func (b *Bybit) mapBybitError(code int, message, body string) error {
	switch code {
	case 10001, 10002, 10003:
		return ccxt.NewAuthenticationError(message)
	case 10004, 10005:
		return ccxt.NewPermissionDenied(message)
	case 10006:
		return ccxt.NewRateLimitExceeded(message, 60)
	case 10007, 10008:
		return ccxt.NewInvalidRequest(message)
	case 10009:
		return ccxt.NewMarketNotFound(message)
	case 10010:
		return ccxt.NewInvalidOrder(message, "")
	case 10011:
		return ccxt.NewOrderNotFound(message)
	case 10012:
		return ccxt.NewInsufficientFunds(message, 0.0, 0.0)
	case 10013:
		return ccxt.NewNetworkError(message)
	case 10014:
		return ccxt.NewExchangeNotAvailable(message)
	case 10015:
		return ccxt.NewRequestTimeout(message)
	case 10016:
		return ccxt.NewBadRequest(message)
	case 110001, 110002:
		return ccxt.NewInvalidOrder(message, "")
	case 110003, 110004:
		return ccxt.NewInsufficientFunds(message, 0.0, 0.0)
	case 110005:
		return ccxt.NewOrderNotFound(message)
	case 170001:
		return ccxt.NewAuthenticationError(message)
	case 170002:
		return ccxt.NewPermissionDenied(message)
	case 170003:
		return ccxt.NewRateLimitExceeded(message, 60)
	case 170004:
		return ccxt.NewNetworkError(message)
	case 170005:
		return ccxt.NewExchangeNotAvailable(message)
	default:
		return ccxt.NewExchangeError(fmt.Sprintf("Bybit error %d: %s", code, message))
	}
}

// ========== 实用工具方法 ==========

// SafeInt 安全获取整数值
func (b *Bybit) SafeInt(data map[string]interface{}, key string, defaultValue int64) int64 {
	return b.BaseExchange.SafeInt(data, key, defaultValue)
}

// symbolInList 检查交易对是否在列表中
func (b *Bybit) symbolInList(symbol string, symbols []string) bool {
	for _, s := range symbols {
		if s == symbol {
			return true
		}
	}
	return false
}

// convertMarginMode 转换保证金模式
func (b *Bybit) convertMarginMode(marginMode string) string {
	switch marginMode {
	case "isolated":
		return "0"
	case "cross":
		return "1"
	default:
		return "0" // 默认逐仓
	}
}

// parseMarginMode 解析保证金模式
func (b *Bybit) parseMarginMode(tradeMode string) string {
	switch tradeMode {
	case "0":
		return "isolated"
	case "1":
		return "cross"
	default:
		return "isolated"
	}
}

// getDefaultChain 获取币种的默认链
func (b *Bybit) getDefaultChain(code string) string {
	chains := map[string]string{
		"BTC":  "BTC",
		"ETH":  "ETH",
		"USDT": "TRX", // Bybit USDT 默认使用TRX链
		"USDC": "ETH",
		"BNB":  "BSC",
	}

	if chain, exists := chains[code]; exists {
		return chain
	}
	return code // 默认返回币种代码
}

// floatToPrecision 浮点数精度处理
func (b *Bybit) floatToPrecision(value, precision float64) float64 {
	if precision <= 0 {
		return value
	}
	// 简单的精度处理
	factor := math.Pow(10, precision)
	return math.Round(value*factor) / factor
}

// formatToPrecision 格式化为字符串
func (b *Bybit) formatToPrecision(value, precision float64) string {
	if precision <= 0 {
		return fmt.Sprintf("%.8f", value)
	}
	return b.BaseExchange.FloatToPrecision(value, int(precision))
}

// ========== 私有工具方法 ==========

// getEndpoint 获取API端点
func (b *Bybit) getEndpoint(name string) string {
	if endpoint, exists := b.endpoints[name]; exists {
		return endpoint
	}
	return ""
}

// buildQuery 构建查询参数
func (b *Bybit) buildQuery(params map[string]interface{}) string {
	if len(params) == 0 {
		return ""
	}
	values := url.Values{}
	for key, value := range params {
		values.Add(key, fmt.Sprintf("%v", value))
	}
	return values.Encode()
}

// loadMarketsIfNeeded 如果需要则加载市场
func (b *Bybit) loadMarketsIfNeeded(ctx context.Context) error {
	if len(b.BaseExchange.GetMarkets()) == 0 {
		_, err := b.LoadMarkets(ctx, false)
		return err
	}
	return nil
}

// getMarket 获取市场信息
func (b *Bybit) getMarket(symbol string) *ccxt.Market {
	markets := b.BaseExchange.GetMarkets()
	return markets[symbol]
}

// getMarketCategory 根据市场类型获取category
func (b *Bybit) getMarketCategory(marketType string) string {
	switch marketType {
	case ccxt.MarketTypeSpot:
		return "spot"
	case ccxt.MarketTypeFuture, ccxt.MarketTypeDerivative:
		if b.config.Category == "inverse" {
			return "inverse"
		}
		return "linear"
	case ccxt.MarketTypeOption:
		return "option"
	default:
		return b.config.Category
	}
}

// ========== 仓位管理 ==========

// FetchPositions 获取持仓
func (b *Bybit) FetchPositions(ctx context.Context, symbols []string, params map[string]interface{}) ([]*ccxt.Position, error) {
	reqParams := map[string]interface{}{
		"category": b.config.Category,
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "GET", "positions", reqParams)
	if err != nil {
		return nil, err
	}

	data, ok := response.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid response format")
	}

	result, ok := data["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid result format")
	}

	listData, ok := result["list"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid list format")
	}

	positions := make([]*ccxt.Position, 0, len(listData))
	for _, item := range listData {
		positionData, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		position := b.parsePosition(positionData)
		if position != nil {
			positions = append(positions, position)
		}
	}

	return positions, nil
}

// Close 关闭连接
func (b *Bybit) Close() error {
	// 清理资源
	b.subscriptions = make(map[string]bool)
	return nil
}

// 修复所有的 PublicAPI 调用，移除多余的 "GET" 参数
// response, err := b.PublicAPI(ctx, "GET", "funding", reqParams)
// 改为：
// response, err := b.PublicAPI(ctx, "funding", reqParams)

// 在 parsers.go 文件中应该有的方法，如果没有则添加到这里
// parseMyTrade 解析我的交易数据
func (b *Bybit) parseMyTrade(trade map[string]interface{}, symbol string) *ccxt.Trade {
	timestamp := b.SafeInt(trade, "execTime", 0)
	price := b.SafeFloat(trade, "execPrice", 0)
	amount := b.SafeFloat(trade, "execQty", 0)

	side := ""
	if execType, ok := trade["side"].(string); ok {
		side = strings.ToLower(execType)
	}

	return &ccxt.Trade{
		ID:        b.SafeString(trade, "execId", ""),
		Symbol:    symbol,
		Timestamp: timestamp,
		Datetime:  b.BaseExchange.ISO8601(timestamp),
		Side:      side,
		Amount:    amount,
		Price:     price,
		Cost:      price * amount,
		Fee: ccxt.Fee{
			Currency: b.SafeString(trade, "feeRate", ""),
			Cost:     b.SafeFloat(trade, "execFee", 0),
		},
		Info: trade,
	}
}

// parseFundingRate 解析资金费率数据
func (b *Bybit) parseFundingRate(funding map[string]interface{}, market *ccxt.Market) *ccxt.FundingRate {
	timestamp := b.SafeInt(funding, "fundingRateTimestamp", 0)

	return &ccxt.FundingRate{
		Symbol:               market.Symbol,
		FundingRate:          b.SafeFloat(funding, "fundingRate", 0),
		Timestamp:            timestamp,
		Datetime:             b.BaseExchange.ISO8601(timestamp),
		FundingTimestamp:     timestamp,
		FundingDatetime:      b.BaseExchange.ISO8601(timestamp),
		NextFundingRate:      b.SafeFloat(funding, "nextFundingRate", 0),
		NextFundingTimestamp: b.SafeInt(funding, "nextFundingTime", 0),
		NextFundingDatetime:  b.BaseExchange.ISO8601(b.SafeInt(funding, "nextFundingTime", 0)),
		Info:                 funding,
	}
}

// parseDepositAddress 解析充值地址
func (b *Bybit) parseDepositAddress(address map[string]interface{}, code string) *ccxt.DepositAddress {
	return &ccxt.DepositAddress{
		Currency: code,
		Address:  b.SafeString(address, "address", ""),
		Tag:      b.SafeString(address, "tag", ""),
		Network:  b.SafeString(address, "chain", ""),
		Info:     address,
	}
}

// parseDeposit 解析充值记录
func (b *Bybit) parseDeposit(deposit map[string]interface{}) *ccxt.Transaction {
	timestamp := b.SafeInt(deposit, "successAt", 0)
	status := b.parseTransactionStatus(b.SafeString(deposit, "status", ""))

	return &ccxt.Transaction{
		ID:        b.SafeString(deposit, "id", ""),
		TxID:      b.SafeString(deposit, "txID", ""),
		Timestamp: timestamp,
		Datetime:  b.BaseExchange.ISO8601(timestamp),
		Network:   b.SafeString(deposit, "chain", ""),
		Address:   b.SafeString(deposit, "address", ""),
		AddressTo: b.SafeString(deposit, "address", ""),
		Tag:       b.SafeString(deposit, "tag", ""),
		TagTo:     b.SafeString(deposit, "tag", ""),
		Type:      "deposit",
		Amount:    b.SafeFloat(deposit, "amount", 0),
		Currency:  b.SafeString(deposit, "coin", ""),
		Status:    status,
		Updated:   timestamp,
		Fee: ccxt.Fee{
			Currency: b.SafeString(deposit, "coin", ""),
			Cost:     0, // 充值通常无手续费
		},
		Info: deposit,
	}
}

// parseWithdraw 解析提现记录
func (b *Bybit) parseWithdraw(withdraw map[string]interface{}) *ccxt.Transaction {
	timestamp := b.SafeInt(withdraw, "createTime", 0)
	status := b.parseTransactionStatus(b.SafeString(withdraw, "status", ""))

	return &ccxt.Transaction{
		ID:          b.SafeString(withdraw, "id", ""),
		TxID:        b.SafeString(withdraw, "txID", ""),
		Timestamp:   timestamp,
		Datetime:    b.BaseExchange.ISO8601(timestamp),
		Network:     b.SafeString(withdraw, "chain", ""),
		Address:     b.SafeString(withdraw, "toAddress", ""),
		AddressFrom: "",
		AddressTo:   b.SafeString(withdraw, "toAddress", ""),
		Tag:         b.SafeString(withdraw, "tag", ""),
		TagFrom:     "",
		TagTo:       b.SafeString(withdraw, "tag", ""),
		Type:        "withdrawal",
		Amount:      b.SafeFloat(withdraw, "amount", 0),
		Currency:    b.SafeString(withdraw, "coin", ""),
		Status:      status,
		Updated:     timestamp,
		Fee: ccxt.Fee{
			Currency: b.SafeString(withdraw, "coin", ""),
			Cost:     b.SafeFloat(withdraw, "withdrawFee", 0),
		},
		Info: withdraw,
	}
}

// parseTransactionStatus 解析交易状态
func (b *Bybit) parseTransactionStatus(status string) string {
	switch status {
	case "0", "ToBeConfirmed":
		return "pending"
	case "1", "Success":
		return "ok"
	case "2", "Fail":
		return "failed"
	case "3", "Pending":
		return "pending"
	default:
		return status
	}
}

// SafeFloat 安全获取浮点数值
func (b *Bybit) SafeFloat(data map[string]interface{}, key string, defaultValue float64) float64 {
	return b.BaseExchange.SafeFloat(data, key, defaultValue)
}

// getAccountType 获取账户类型
func (b *Bybit) getAccountType() string {
	switch b.config.Category {
	case "spot":
		return "SPOT"
	case "linear":
		return "CONTRACT"
	case "inverse":
		return "INVERSE"
	case "option":
		return "OPTION"
	default:
		return "UNIFIED" // 统一账户
	}
}

// mapOrderType 映射订单类型
func (b *Bybit) mapOrderType(orderType string) string {
	switch orderType {
	case ccxt.OrderTypeMarket:
		return "Market"
	case ccxt.OrderTypeLimit:
		return "Limit"
	default:
		return "Limit"
	}
}

// formatAmount 格式化数量
func (b *Bybit) formatAmount(amount float64, market *ccxt.Market) string {
	if market != nil && market.Precision.Amount > 0 {
		return b.BaseExchange.FloatToPrecision(amount, int(market.Precision.Amount))
	}
	return fmt.Sprintf("%.8f", amount)
}

// formatPrice 格式化价格
func (b *Bybit) formatPrice(price float64, market *ccxt.Market) string {
	if market != nil && market.Precision.Price > 0 {
		return b.BaseExchange.FloatToPrecision(price, int(market.Precision.Price))
	}
	return fmt.Sprintf("%.8f", price)
}

// PrivateAPI 私有API请求
func (b *Bybit) PrivateAPI(ctx context.Context, method, endpoint string, params map[string]interface{}) (interface{}, error) {
	return nil, ccxt.NewNotSupported("PrivateAPI")
}
