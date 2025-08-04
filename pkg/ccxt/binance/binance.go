package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"datahive/pkg/ccxt"
)

// 注册Binance交易所
func init() {
	ccxt.RegisterExchange("binance", func(config ccxt.ExchangeConfig) (ccxt.Exchange, error) {
		// 将通用配置转换为Binance内部配置
		binanceConfig := &Config{
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
			RecvWindow:      5000, // Binance特有配置
		}

		// 如果是BaseConfig，可以获取额外的字段
		if baseConfig, ok := config.(*ccxt.BaseConfig); ok {
			binanceConfig.DefaultType = baseConfig.DefaultType
			binanceConfig.EnableWebSocket = baseConfig.EnableWebSocket
			binanceConfig.WSMaxReconnect = baseConfig.WSMaxReconnect
			binanceConfig.RecvWindow = baseConfig.RecvWindow
		} else {
			// 非BaseConfig的兜底默认值
			binanceConfig.DefaultType = "spot"
			binanceConfig.EnableWebSocket = false
		}

		return New(binanceConfig)
	})
}

// ========== Binance 交易所实现 ==========

// Binance 实现完整的CCXT交易所接口
type Binance struct {
	*ccxt.BaseExchange
	config *Config

	// API端点缓存
	endpoints map[string]string

	// 订阅管理
	subscriptions map[string]bool

	// WebSocket客户端
	wsClient *BinanceWebSocket

	// 缓存字段
	lastServerTimeRequest int64
	serverTimeOffset      int64
}

// ========== 构造函数 ==========

// New 创建新的Binance实例
func New(config *Config) (*Binance, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	base := ccxt.NewBaseExchange("binance", "Binance", "v3", []string{"JP", "MT"})
	binance := &Binance{
		BaseExchange:  base,
		config:        config.Clone(),
		endpoints:     make(map[string]string),
		subscriptions: make(map[string]bool),
	}

	// 初始化WebSocket客户端
	if config.EnableWebSocket {
		binance.wsClient = NewBinanceWebSocket(binance)
	}

	// 设置基础信息
	binance.SetBasicInfo()

	// 设置支持的功能
	binance.SetCapabilities()

	// 设置时间周期
	binance.SetTimeframes()

	// 设置API端点
	binance.SetEndpoints()

	// 设置费率信息
	binance.SetFees()

	return binance, nil
}

// ========== 基础配置方法 ==========

// SetBasicInfo 设置基础信息
func (b *Binance) SetBasicInfo() {
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
func (b *Binance) SetCapabilities() {
	capabilities := map[string]bool{
		// 基础功能
		"loadMarkets":     true,
		"fetchMarkets":    true,
		"fetchCurrencies": true,
		"fetchTicker":     true,
		"fetchTickers":    true,
		"fetchOrderBook":  true,
		"fetchTrades":     true,
		"fetchOHLCV":      true,
		"fetchStatus":     true,
		"fetchTime":       true,

		// 账户功能
		"fetchBalance":     true,
		"fetchTradingFees": true,
		"fetchFundingFees": true,

		// 交易功能
		"createOrder":       true,
		"createMarketOrder": true,
		"createLimitOrder":  true,
		"editOrder":         false, // Binance不支持修改订单
		"cancelOrder":       true,
		"cancelAllOrders":   true,
		"fetchOrder":        true,
		"fetchOrders":       true,
		"fetchOpenOrders":   true,
		"fetchClosedOrders": true,
		"fetchMyTrades":     true,

		// 资金功能
		"fetchDepositAddress": true,
		"fetchDeposits":       true,
		"fetchWithdrawals":    true,
		"withdraw":            true,

		// 期货功能
		"fetchPositions":      true,
		"setLeverage":         true,
		"setMarginMode":       true,
		"fetchFundingRate":    true,
		"fetchFundingRates":   true,
		"fetchFundingHistory": true,

		// WebSocket功能
		"watchTicker":    b.config.EnableWebSocket,
		"watchTickers":   b.config.EnableWebSocket,
		"watchOrderBook": b.config.EnableWebSocket,
		"watchTrades":    b.config.EnableWebSocket,
		"watchOHLCV":     b.config.EnableWebSocket,
		"watchBalance":   b.config.EnableWebSocket,
		"watchOrders":    b.config.EnableWebSocket,

		// 市场类型
		"spot":   true,
		"margin": true,
		"future": true,
		"option": false,
		"swap":   true,
	}

	b.BaseExchange.SetCapabilities(capabilities)
}

// SetTimeframes 设置支持的时间周期
func (b *Binance) SetTimeframes() {
	timeframes := map[string]string{
		"1s":  "1s",
		"1m":  "1m",
		"3m":  "3m",
		"5m":  "5m",
		"15m": "15m",
		"30m": "30m",
		"1h":  "1h",
		"2h":  "2h",
		"4h":  "4h",
		"6h":  "6h",
		"8h":  "8h",
		"12h": "12h",
		"1d":  "1d",
		"3d":  "3d",
		"1w":  "1w",
		"1M":  "1M",
	}

	b.BaseExchange.SetTimeframes(timeframes)
}

// SetEndpoints 设置API端点
func (b *Binance) SetEndpoints() {
	baseURL := b.config.GetBaseURL()
	futuresURL := b.config.GetFuturesURL()

	b.endpoints = map[string]string{
		// 基础信息
		"ping":         baseURL + "/api/v3/ping",
		"time":         baseURL + "/api/v3/time",
		"exchangeInfo": baseURL + "/api/v3/exchangeInfo",

		// 市场数据
		"ticker24hr":  baseURL + "/api/v3/ticker/24hr",
		"tickerPrice": baseURL + "/api/v3/ticker/price",
		"bookTicker":  baseURL + "/api/v3/ticker/bookTicker",
		"depth":       baseURL + "/api/v3/depth",
		"trades":      baseURL + "/api/v3/trades",
		"aggTrades":   baseURL + "/api/v3/aggTrades",
		"klines":      baseURL + "/api/v3/klines",
		"avgPrice":    baseURL + "/api/v3/avgPrice",

		// 账户和交易
		"account":    baseURL + "/api/v3/account",
		"openOrders": baseURL + "/api/v3/openOrders",
		"allOrders":  baseURL + "/api/v3/allOrders",
		"order":      baseURL + "/api/v3/order",
		"orderTest":  baseURL + "/api/v3/order/test",
		"myTrades":   baseURL + "/api/v3/myTrades",

		// 钱包
		"capitalConfig":   baseURL + "/sapi/v1/capital/config/getall",
		"depositAddress":  baseURL + "/sapi/v1/capital/deposit/address",
		"depositHistory":  baseURL + "/sapi/v1/capital/deposit/hisrec",
		"withdraw":        baseURL + "/sapi/v1/capital/withdraw/apply",
		"withdrawHistory": baseURL + "/sapi/v1/capital/withdraw/history",
		"assetTradeFee":   baseURL + "/sapi/v1/asset/tradeFee",

		// 杠杆
		"marginAccount":    baseURL + "/sapi/v1/margin/account",
		"marginOrder":      baseURL + "/sapi/v1/margin/order",
		"marginOpenOrders": baseURL + "/sapi/v1/margin/openOrders",
		"marginAllOrders":  baseURL + "/sapi/v1/margin/allOrders",
		"marginMyTrades":   baseURL + "/sapi/v1/margin/myTrades",

		// 期货
		"futuresExchangeInfo": futuresURL + "/fapi/v1/exchangeInfo",
		"futuresAccount":      futuresURL + "/fapi/v2/account",
		"futuresBalance":      futuresURL + "/fapi/v2/balance",
		"futuresPositionRisk": futuresURL + "/fapi/v2/positionRisk",
		"futuresOrder":        futuresURL + "/fapi/v1/order",
		"futuresOpenOrders":   futuresURL + "/fapi/v1/openOrders",
		"futuresAllOrders":    futuresURL + "/fapi/v1/allOrders",
		"futuresUserTrades":   futuresURL + "/fapi/v1/userTrades",
		"futuresLeverage":     futuresURL + "/fapi/v1/leverage",
		"futuresMarginType":   futuresURL + "/fapi/v1/marginType",
		"futuresPremiumIndex": futuresURL + "/fapi/v1/premiumIndex",
		"futuresFundingRate":  futuresURL + "/fapi/v1/fundingRate",
	}
}

// SetFees 设置费率信息
func (b *Binance) SetFees() {
	fees := map[string]map[string]interface{}{
		"trading": {
			"tierBased":  false,
			"percentage": true,
			"maker":      0.001,
			"taker":      0.001,
		},
		"funding": {
			"tierBased":  false,
			"percentage": false,
			"withdraw":   map[string]interface{}{},
			"deposit":    map[string]interface{}{},
		},
	}

	b.BaseExchange.SetFees(fees)
}

// ========== 认证和签名 ==========

// Sign 对请求进行签名
func (b *Binance) Sign(path, api, method string, params map[string]interface{}, headers map[string]string, body interface{}) (string, map[string]string, interface{}, error) {
	url := path

	if headers == nil {
		headers = make(map[string]string)
	}

	// 公开API不需要签名
	if api == "public" {
		if len(params) > 0 {
			query := b.buildQueryString(params)
			if query != "" {
				if strings.Contains(url, "?") {
					url += "&" + query
				} else {
					url += "?" + query
				}
			}
		}
		return url, headers, body, nil
	}

	// 私有API需要签名
	if b.config.APIKey == "" {
		return "", nil, nil, ccxt.NewAuthenticationError("API key required")
	}

	headers["X-MBX-APIKEY"] = b.config.APIKey

	// 添加时间戳
	timestamp := b.Milliseconds()
	params["timestamp"] = timestamp

	// 添加接收窗口
	if b.config.RecvWindow > 0 {
		params["recvWindow"] = b.config.RecvWindow
	}

	// 构建查询字符串
	query := b.buildQueryString(params)

	// 签名
	if b.config.Secret != "" {
		signature := b.hmacSHA256(query, b.config.Secret)
		query += "&signature=" + signature
	}

	if method == "GET" || method == "DELETE" {
		if strings.Contains(url, "?") {
			url += "&" + query
		} else {
			url += "?" + query
		}
	} else {
		// POST/PUT 请求
		headers["Content-Type"] = "application/x-www-form-urlencoded"
		body = query
	}

	return url, headers, body, nil
}

// buildQueryString 构建查询字符串
func (b *Binance) buildQueryString(params map[string]interface{}) string {
	if len(params) == 0 {
		return ""
	}

	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := params[k]
		if v != nil {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
	}

	return strings.Join(parts, "&")
}

// hmacSHA256 HMAC-SHA256签名
func (b *Binance) hmacSHA256(data, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

// ========== 市场数据方法 ==========

// LoadMarkets 加载市场信息
func (b *Binance) LoadMarkets(ctx context.Context, reload ...bool) (map[string]*ccxt.Market, error) {
	// 处理可变参数
	shouldReload := false
	if len(reload) > 0 {
		shouldReload = reload[0]
	}

	if !shouldReload && len(b.BaseExchange.GetMarkets()) > 0 {
		return b.BaseExchange.GetMarkets(), nil
	}

	// 获取现货市场
	spotMarkets, err := b.fetchSpotMarkets(ctx)
	if err != nil {
		return nil, err
	}

	// 如果支持期货，也获取期货市场
	allMarkets := spotMarkets
	if b.HasAPI("future") {
		futuresMarkets, err := b.fetchFuturesMarkets(ctx)
		if err == nil { // 期货市场失败不影响现货
			for k, v := range futuresMarkets {
				allMarkets[k] = v
			}
		}
	}

	b.BaseExchange.SetMarkets(allMarkets)
	return allMarkets, nil
}

// fetchSpotMarkets 获取现货市场
func (b *Binance) fetchSpotMarkets(ctx context.Context) (map[string]*ccxt.Market, error) {
	response, err := b.httpGet(ctx, b.endpoints["exchangeInfo"], nil)
	if err != nil {
		return nil, err
	}

	var exchangeInfo ExchangeInfoResponse
	if err := json.Unmarshal([]byte(response), &exchangeInfo); err != nil {
		return nil, err
	}

	markets := make(map[string]*ccxt.Market)
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.Status != "TRADING" {
			continue
		}

		market := b.parseSpotMarket(&symbol)
		if market != nil {
			markets[market.Symbol] = market
		}
	}

	return markets, nil
}

// fetchFuturesMarkets 获取期货市场
func (b *Binance) fetchFuturesMarkets(ctx context.Context) (map[string]*ccxt.Market, error) {
	response, err := b.httpGet(ctx, b.endpoints["futuresExchangeInfo"], nil)
	if err != nil {
		return nil, err
	}

	var exchangeInfo FuturesExchangeInfoResponse
	if err := json.Unmarshal([]byte(response), &exchangeInfo); err != nil {
		return nil, err
	}

	markets := make(map[string]*ccxt.Market)
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.Status != "TRADING" {
			continue
		}

		market := b.parseFuturesMarket(&symbol)
		if market != nil {
			markets[market.Symbol] = market
		}
	}

	return markets, nil
}

// FetchTicker 获取ticker
func (b *Binance) FetchTicker(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.Ticker, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 合并用户参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.httpGet(ctx, b.endpoints["ticker24hr"], reqParams)
	if err != nil {
		return nil, err
	}

	var tickerData Ticker24HrResponse
	if err := json.Unmarshal([]byte(response), &tickerData); err != nil {
		return nil, err
	}

	return b.parseTicker(&tickerData, market), nil
}

// FetchTickers 获取多个ticker信息
func (b *Binance) FetchTickers(ctx context.Context, symbols []string, params map[string]interface{}) (map[string]*ccxt.Ticker, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	var reqParams map[string]interface{}
	if len(symbols) > 0 {
		// 如果指定了symbols，转换为Binance的symbol格式
		binanceSymbols := make([]string, 0, len(symbols))
		for _, symbol := range symbols {
			if market, err := b.getMarket(symbol); err == nil {
				binanceSymbols = append(binanceSymbols, market.ID)
			}
		}
		if len(binanceSymbols) > 0 {
			reqParams = map[string]interface{}{
				"symbols": fmt.Sprintf("[\"%s\"]", strings.Join(binanceSymbols, "\",\"")),
			}
		}
	}

	response, err := b.httpGet(ctx, b.endpoints["ticker24hr"], reqParams)
	if err != nil {
		return nil, err
	}

	var tickersData []Ticker24HrResponse
	if err := json.Unmarshal([]byte(response), &tickersData); err != nil {
		return nil, err
	}

	result := make(map[string]*ccxt.Ticker)
	for _, tickerData := range tickersData {
		if market := b.getMarketByID(tickerData.Symbol); market != nil {
			ticker := b.parseTicker(&tickerData, market)
			result[market.Symbol] = ticker
		}
	}

	return result, nil
}

// FetchOrderBook 获取订单簿
func (b *Binance) FetchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (*ccxt.OrderBook, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"symbol": market.ID,
	}

	if limit > 0 {
		// Binance支持的limit值: 5, 10, 20, 50, 100, 500, 1000, 5000
		validLimits := []int{5, 10, 20, 50, 100, 500, 1000, 5000}
		for _, validLimit := range validLimits {
			if limit <= validLimit {
				reqParams["limit"] = validLimit
				break
			}
		}
	}

	// 添加自定义参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.httpGet(ctx, b.endpoints["depth"], reqParams)
	if err != nil {
		return nil, err
	}

	var depthData DepthResponse
	if err := json.Unmarshal([]byte(response), &depthData); err != nil {
		return nil, err
	}

	return b.parseOrderBook(&depthData, symbol), nil
}

// FetchTrades 获取最近交易
func (b *Binance) FetchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Trade, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	reqParams := map[string]interface{}{
		"symbol": market.ID,
	}

	if limit > 0 {
		reqParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.httpGet(ctx, b.endpoints["trades"], reqParams)
	if err != nil {
		return nil, err
	}

	var tradesData []TradeResponse
	if err := json.Unmarshal([]byte(response), &tradesData); err != nil {
		return nil, err
	}

	trades := make([]*ccxt.Trade, len(tradesData))
	for i, tradeData := range tradesData {
		trades[i] = b.parseTrade(&tradeData, symbol)
	}

	return trades, nil
}

// FetchOHLCV 获取K线数据
func (b *Binance) FetchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) ([]*ccxt.OHLCV, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	binanceTimeframe, exists := b.GetTimeframes()[timeframe]
	if !exists {
		return nil, ccxt.NewBadRequest(fmt.Sprintf("timeframe %s not supported", timeframe))
	}

	reqParams := map[string]interface{}{
		"symbol":   market.ID,
		"interval": binanceTimeframe,
	}

	if since > 0 {
		reqParams["startTime"] = since
	}

	if limit > 0 {
		reqParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		reqParams[k] = v
	}

	response, err := b.httpGet(ctx, b.endpoints["klines"], reqParams)
	if err != nil {
		return nil, err
	}

	var klinesData KlineResponse
	if err := json.Unmarshal([]byte(response), &klinesData); err != nil {
		return nil, err
	}

	klines := make([]*ccxt.OHLCV, len(klinesData))
	for i, klineData := range klinesData {
		klines[i] = b.parseKline(klineData)
	}

	return klines, nil
}

// ========== 账户相关方法 ==========

// FetchBalance 获取账户余额
func (b *Binance) FetchBalance(ctx context.Context, params map[string]interface{}) (*ccxt.Account, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	response, err := b.PrivateAPI(ctx, "account", params)
	if err != nil {
		return nil, err
	}

	var accountData AccountResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &accountData); err != nil {
		return nil, err
	}

	return b.parseSpotBalance(&accountData), nil
}

// ========== 交易相关方法 ==========

// CreateOrder 创建订单
func (b *Binance) CreateOrder(ctx context.Context, symbol, orderType, side string, amount, price float64, params map[string]interface{}) (*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	// 构建订单参数
	orderParams := map[string]interface{}{
		"symbol":   market.ID,
		"side":     strings.ToUpper(side),
		"type":     strings.ToUpper(orderType),
		"quantity": b.AmountToPrecision(symbol, amount),
	}

	if orderType == ccxt.OrderTypeLimit {
		if price <= 0 {
			return nil, ccxt.NewInvalidOrder("price required for limit orders", "")
		}
		orderParams["price"] = b.PriceToPrecision(symbol, price)
		orderParams["timeInForce"] = "GTC" // 默认GTC
	}

	// 添加自定义参数
	for k, v := range params {
		orderParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "order", orderParams)
	if err != nil {
		return nil, err
	}

	var orderData OrderResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &orderData); err != nil {
		return nil, err
	}

	return b.parseOrder(&orderData, market), nil
}

// CancelOrder 取消订单
func (b *Binance) CancelOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	cancelParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 支持orderId或origClientOrderId
	if strings.HasPrefix(id, "x-") {
		cancelParams["origClientOrderId"] = id
	} else {
		if orderID, err := strconv.ParseInt(id, 10, 64); err == nil {
			cancelParams["orderId"] = orderID
		} else {
			cancelParams["origClientOrderId"] = id
		}
	}

	// 添加自定义参数
	for k, v := range params {
		cancelParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "order", cancelParams)
	if err != nil {
		return nil, err
	}

	var orderData OrderResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &orderData); err != nil {
		return nil, err
	}

	return b.parseOrder(&orderData, market), nil
}

// FetchOrder 获取订单信息
func (b *Binance) FetchOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	orderParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 支持orderId或origClientOrderId
	if strings.HasPrefix(id, "x-") {
		orderParams["origClientOrderId"] = id
	} else {
		if orderID, err := strconv.ParseInt(id, 10, 64); err == nil {
			orderParams["orderId"] = orderID
		} else {
			orderParams["origClientOrderId"] = id
		}
	}

	// 添加自定义参数
	for k, v := range params {
		orderParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "order", orderParams)
	if err != nil {
		return nil, err
	}

	var orderData OrderResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &orderData); err != nil {
		return nil, err
	}

	return b.parseOrder(&orderData, market), nil
}

// FetchOpenOrders 获取开放订单
func (b *Binance) FetchOpenOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	orderParams := make(map[string]interface{})

	if symbol != "" {
		if err := b.loadMarketsIfNeeded(ctx); err != nil {
			return nil, err
		}

		market, err := b.getMarket(symbol)
		if err != nil {
			return nil, err
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		orderParams["symbol"] = market.ID
	}

	// 添加自定义参数
	for k, v := range params {
		orderParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "openOrders", orderParams)
	if err != nil {
		return nil, err
	}

	var ordersData []OrderResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &ordersData); err != nil {
		return nil, err
	}

	orders := make([]*ccxt.Order, len(ordersData))
	for i, orderData := range ordersData {
		var market *ccxt.Market
		if symbol != "" {
			market, _ = b.getMarket(symbol)
		} else {
			market = b.getMarketByID(orderData.Symbol)
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(orderData.Symbol)
		}
		orders[i] = b.parseOrder(&orderData, market)
	}

	return orders, nil
}

// FetchClosedOrders 获取已完成的订单
func (b *Binance) FetchClosedOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Order, error) {
	allOrders, err := b.FetchAllOrders(ctx, symbol, since, limit, params)
	if err != nil {
		return nil, err
	}

	// 过滤出已完成的订单
	var closedOrders []*ccxt.Order
	for _, order := range allOrders {
		if order.Status == ccxt.OrderStatusFilled || order.Status == ccxt.OrderStatusCanceled {
			closedOrders = append(closedOrders, order)
		}
	}

	return closedOrders, nil
}

// FetchMyTrades 获取我的交易记录
func (b *Binance) FetchMyTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Trade, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	tradeParams := make(map[string]interface{})

	if symbol != "" {
		market, err := b.getMarket(symbol)
		if err != nil {
			return nil, err
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		tradeParams["symbol"] = market.ID
	}

	if since > 0 {
		tradeParams["startTime"] = since
	}

	if limit > 0 {
		tradeParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		tradeParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "myTrades", tradeParams)
	if err != nil {
		return nil, err
	}

	var tradesData []MyTradeResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &tradesData); err != nil {
		return nil, err
	}

	trades := make([]*ccxt.Trade, len(tradesData))
	for i, tradeData := range tradesData {
		trades[i] = b.parseMyTrade(&tradeData, symbol)
	}

	return trades, nil
}

// FetchAllOrders 获取所有订单
func (b *Binance) FetchAllOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	orderParams := make(map[string]interface{})

	if symbol != "" {
		market, err := b.getMarket(symbol)
		if err != nil {
			return nil, err
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		orderParams["symbol"] = market.ID
	}

	if since > 0 {
		orderParams["startTime"] = since
	}

	if limit > 0 {
		orderParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		orderParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "allOrders", orderParams)
	if err != nil {
		return nil, err
	}

	var ordersData []OrderResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &ordersData); err != nil {
		return nil, err
	}

	orders := make([]*ccxt.Order, len(ordersData))
	for i, orderData := range ordersData {
		var market *ccxt.Market
		if symbol != "" {
			market, _ = b.getMarket(symbol)
		} else {
			market = b.getMarketByID(orderData.Symbol)
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(orderData.Symbol)
		}
		orders[i] = b.parseOrder(&orderData, market)
	}

	return orders, nil
}

// CancelAllOrders 取消所有订单
func (b *Binance) CancelAllOrders(ctx context.Context, symbol string, params map[string]interface{}) ([]*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	cancelParams := make(map[string]interface{})

	if symbol != "" {
		market, err := b.getMarket(symbol)
		if err != nil {
			return nil, err
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
		}
		cancelParams["symbol"] = market.ID
	}

	// 添加自定义参数
	for k, v := range params {
		cancelParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "openOrders", cancelParams)
	if err != nil {
		return nil, err
	}

	var ordersData []OrderResponse
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &ordersData); err != nil {
		return nil, err
	}

	orders := make([]*ccxt.Order, len(ordersData))
	for i, orderData := range ordersData {
		var market *ccxt.Market
		if symbol != "" {
			market, _ = b.getMarket(symbol)
		} else {
			market = b.getMarketByID(orderData.Symbol)
		}
		if market == nil {
			return nil, ccxt.NewMarketNotFound(orderData.Symbol)
		}
		orders[i] = b.parseOrder(&orderData, market)
	}

	return orders, nil
}

// ========== 资金相关方法 ==========

// FetchDepositAddress 获取充值地址
func (b *Binance) FetchDepositAddress(ctx context.Context, code string, params map[string]interface{}) (*ccxt.DepositAddress, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	addressParams := map[string]interface{}{
		"coin": code,
	}

	// 添加网络参数
	if network, exists := params["network"]; exists {
		addressParams["network"] = network
	}

	// 添加自定义参数
	for k, v := range params {
		addressParams[k] = v
	}

	url := "/sapi/v1/capital/deposit/address"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", addressParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		Address string `json:"address"`
		Coin    string `json:"coin"`
		Tag     string `json:"tag"`
		URL     string `json:"url"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	return &ccxt.DepositAddress{
		Currency: code,
		Address:  response.Address,
		Tag:      response.Tag,
		Network:  b.SafeString(params, "network", ""),
		Info:     map[string]interface{}{"coin": response.Coin, "address": response.Address, "tag": response.Tag, "url": response.URL},
	}, nil
}

// FetchDepositAddressesByNetwork 获取多网络充值地址
func (b *Binance) FetchDepositAddressesByNetwork(ctx context.Context, code string, params map[string]interface{}) (map[string]*ccxt.DepositAddress, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	// 首先获取支持的网络列表
	coinInfoParams := map[string]interface{}{}

	url := "/sapi/v1/capital/config/getall"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", coinInfoParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var coins []struct {
		Coin              string `json:"coin"`
		DepositAllEnable  bool   `json:"depositAllEnable"`
		WithdrawAllEnable bool   `json:"withdrawAllEnable"`
		Name              string `json:"name"`
		Free              string `json:"free"`
		Locked            string `json:"locked"`
		Freeze            string `json:"freeze"`
		Withdrawing       string `json:"withdrawing"`
		Ipoing            string `json:"ipoing"`
		Ipoable           string `json:"ipoable"`
		Storage           string `json:"storage"`
		IsLegalMoney      bool   `json:"isLegalMoney"`
		Trading           bool   `json:"trading"`
		NetworkList       []struct {
			Network                 string `json:"network"`
			Coin                    string `json:"coin"`
			WithdrawIntegerMultiple string `json:"withdrawIntegerMultiple"`
			IsDefault               bool   `json:"isDefault"`
			DepositEnable           bool   `json:"depositEnable"`
			WithdrawEnable          bool   `json:"withdrawEnable"`
			DepositDesc             string `json:"depositDesc"`
			WithdrawDesc            string `json:"withdrawDesc"`
			SpecialTips             string `json:"specialTips"`
			SpecialWithdrawTips     string `json:"specialWithdrawTips"`
			Name                    string `json:"name"`
			ResetAddressStatus      bool   `json:"resetAddressStatus"`
			AddressRegex            string `json:"addressRegex"`
			AddressRule             string `json:"addressRule"`
			MemoRegex               string `json:"memoRegex"`
			WithdrawFee             string `json:"withdrawFee"`
			WithdrawMin             string `json:"withdrawMin"`
			WithdrawMax             string `json:"withdrawMax"`
			MinConfirm              int    `json:"minConfirm"`
			UnLockConfirm           int    `json:"unLockConfirm"`
			SameAddress             bool   `json:"sameAddress"`
		} `json:"networkList"`
	}
	if err := json.Unmarshal([]byte(respBody), &coins); err != nil {
		return nil, err
	}

	// 查找指定币种的网络列表
	var targetCoin *struct {
		Coin              string `json:"coin"`
		DepositAllEnable  bool   `json:"depositAllEnable"`
		WithdrawAllEnable bool   `json:"withdrawAllEnable"`
		Name              string `json:"name"`
		Free              string `json:"free"`
		Locked            string `json:"locked"`
		Freeze            string `json:"freeze"`
		Withdrawing       string `json:"withdrawing"`
		Ipoing            string `json:"ipoing"`
		Ipoable           string `json:"ipoable"`
		Storage           string `json:"storage"`
		IsLegalMoney      bool   `json:"isLegalMoney"`
		Trading           bool   `json:"trading"`
		NetworkList       []struct {
			Network                 string `json:"network"`
			Coin                    string `json:"coin"`
			WithdrawIntegerMultiple string `json:"withdrawIntegerMultiple"`
			IsDefault               bool   `json:"isDefault"`
			DepositEnable           bool   `json:"depositEnable"`
			WithdrawEnable          bool   `json:"withdrawEnable"`
			DepositDesc             string `json:"depositDesc"`
			WithdrawDesc            string `json:"withdrawDesc"`
			SpecialTips             string `json:"specialTips"`
			SpecialWithdrawTips     string `json:"specialWithdrawTips"`
			Name                    string `json:"name"`
			ResetAddressStatus      bool   `json:"resetAddressStatus"`
			AddressRegex            string `json:"addressRegex"`
			AddressRule             string `json:"addressRule"`
			MemoRegex               string `json:"memoRegex"`
			WithdrawFee             string `json:"withdrawFee"`
			WithdrawMin             string `json:"withdrawMin"`
			WithdrawMax             string `json:"withdrawMax"`
			MinConfirm              int    `json:"minConfirm"`
			UnLockConfirm           int    `json:"unLockConfirm"`
			SameAddress             bool   `json:"sameAddress"`
		} `json:"networkList"`
	}

	for i := range coins {
		if coins[i].Coin == code {
			targetCoin = &coins[i]
			break
		}
	}

	if targetCoin == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("currency %s not found", code))
	}

	result := make(map[string]*ccxt.DepositAddress)

	// 为每个支持的网络获取充值地址
	for _, network := range targetCoin.NetworkList {
		if !network.DepositEnable {
			continue // 跳过不支持充值的网络
		}

		networkParams := map[string]interface{}{
			"network": network.Network,
		}
		for k, v := range params {
			networkParams[k] = v
		}

		depositAddress, err := b.FetchDepositAddress(ctx, code, networkParams)
		if err != nil {
			continue // 跳过获取失败的网络
		}

		result[network.Network] = depositAddress
	}

	return result, nil
}

// Withdraw 提现
func (b *Binance) Withdraw(ctx context.Context, code string, amount float64, address string, tag string, params map[string]interface{}) (*ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	withdrawParams := map[string]interface{}{
		"coin":    code,
		"address": address,
		"amount":  amount,
	}

	if tag != "" {
		withdrawParams["addressTag"] = tag
	}

	// 添加网络参数（如果指定）
	if network, exists := params["network"]; exists {
		withdrawParams["network"] = network
	}

	// 添加提现名称（如果指定）
	if name, exists := params["name"]; exists {
		withdrawParams["name"] = name
	}

	// 添加自定义参数
	for k, v := range params {
		withdrawParams[k] = v
	}

	url := "/sapi/v1/capital/withdraw/apply"
	signedURL, headers, body, err := b.Sign(url, "private", "POST", withdrawParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	return &ccxt.Transaction{
		ID:        response.ID,
		TxID:      "",
		Currency:  code,
		Amount:    amount,
		Address:   address,
		Tag:       tag,
		Type:      "withdrawal",
		Status:    "pending",
		Network:   b.SafeString(params, "network", ""),
		Timestamp: b.Milliseconds(),
		Datetime:  b.ISO8601(b.Milliseconds()),
		Info:      map[string]interface{}{"id": response.ID},
	}, nil
}

// FetchDeposits 获取充值记录
func (b *Binance) FetchDeposits(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	depositParams := make(map[string]interface{})

	if code != "" {
		depositParams["coin"] = code
	}

	if since > 0 {
		depositParams["startTime"] = since
	}

	if limit > 0 {
		depositParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		depositParams[k] = v
	}

	url := "/sapi/v1/capital/deposit/hisrec"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", depositParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var deposits []struct {
		ID            string `json:"id"`
		Amount        string `json:"amount"`
		Coin          string `json:"coin"`
		Network       string `json:"network"`
		Status        int    `json:"status"`
		Address       string `json:"address"`
		AddressTag    string `json:"addressTag"`
		TxID          string `json:"txId"`
		InsertTime    int64  `json:"insertTime"`
		TransferType  int    `json:"transferType"`
		UnlockConfirm string `json:"unlockConfirm"`
		ConfirmTimes  string `json:"confirmTimes"`
	}
	if err := json.Unmarshal([]byte(respBody), &deposits); err != nil {
		return nil, err
	}

	var result []ccxt.Transaction
	for _, deposit := range deposits {
		status := b.parseTransactionStatus(deposit.Status)
		amount := b.SafeFloat(map[string]interface{}{"amount": deposit.Amount}, "amount", 0)

		transaction := ccxt.Transaction{
			ID:        deposit.ID,
			TxID:      deposit.TxID,
			Currency:  deposit.Coin,
			Amount:    amount,
			Address:   deposit.Address,
			Tag:       deposit.AddressTag,
			Type:      "deposit",
			Status:    status,
			Network:   deposit.Network,
			Timestamp: deposit.InsertTime,
			Datetime:  b.ISO8601(deposit.InsertTime),
			Info: map[string]interface{}{
				"id":            deposit.ID,
				"coin":          deposit.Coin,
				"network":       deposit.Network,
				"status":        deposit.Status,
				"address":       deposit.Address,
				"addressTag":    deposit.AddressTag,
				"txId":          deposit.TxID,
				"insertTime":    deposit.InsertTime,
				"transferType":  deposit.TransferType,
				"unlockConfirm": deposit.UnlockConfirm,
				"confirmTimes":  deposit.ConfirmTimes,
			},
		}
		result = append(result, transaction)
	}

	return result, nil
}

// FetchWithdrawals 获取提现记录
func (b *Binance) FetchWithdrawals(ctx context.Context, code string, since int64, limit int, params map[string]interface{}) ([]ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	withdrawParams := make(map[string]interface{})

	if code != "" {
		withdrawParams["coin"] = code
	}

	if since > 0 {
		withdrawParams["startTime"] = since
	}

	if limit > 0 {
		withdrawParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		withdrawParams[k] = v
	}

	url := "/sapi/v1/capital/withdraw/history"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", withdrawParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var withdrawals []struct {
		ID              string `json:"id"`
		Amount          string `json:"amount"`
		TransactionFee  string `json:"transactionFee"`
		Coin            string `json:"coin"`
		Status          int    `json:"status"`
		Address         string `json:"address"`
		AddressTag      string `json:"addressTag"`
		TxID            string `json:"txId"`
		ApplyTime       string `json:"applyTime"`
		Network         string `json:"network"`
		WithdrawOrderId string `json:"withdrawOrderId"`
		Info            string `json:"info"`
		TransferType    int    `json:"transferType"`
		ConfirmNo       int    `json:"confirmNo"`
	}
	if err := json.Unmarshal([]byte(respBody), &withdrawals); err != nil {
		return nil, err
	}

	var result []ccxt.Transaction
	for _, withdrawal := range withdrawals {
		status := b.parseTransactionStatus(withdrawal.Status)
		amount := b.SafeFloat(map[string]interface{}{"amount": withdrawal.Amount}, "amount", 0)
		fee := b.SafeFloat(map[string]interface{}{"fee": withdrawal.TransactionFee}, "fee", 0)
		timestamp, _ := strconv.ParseInt(withdrawal.ApplyTime, 10, 64)

		transaction := ccxt.Transaction{
			ID:        withdrawal.ID,
			TxID:      withdrawal.TxID,
			Currency:  withdrawal.Coin,
			Amount:    amount,
			Address:   withdrawal.Address,
			Tag:       withdrawal.AddressTag,
			Type:      "withdrawal",
			Status:    status,
			Network:   withdrawal.Network,
			Timestamp: timestamp,
			Datetime:  b.ISO8601(timestamp),
			Fee: ccxt.Fee{
				Currency: withdrawal.Coin,
				Cost:     fee,
			},
			Info: map[string]interface{}{
				"id":              withdrawal.ID,
				"coin":            withdrawal.Coin,
				"status":          withdrawal.Status,
				"address":         withdrawal.Address,
				"addressTag":      withdrawal.AddressTag,
				"txId":            withdrawal.TxID,
				"applyTime":       withdrawal.ApplyTime,
				"network":         withdrawal.Network,
				"withdrawOrderId": withdrawal.WithdrawOrderId,
				"info":            withdrawal.Info,
				"transferType":    withdrawal.TransferType,
				"confirmNo":       withdrawal.ConfirmNo,
				"transactionFee":  withdrawal.TransactionFee,
			},
		}
		result = append(result, transaction)
	}

	return result, nil
}

// parseTransactionStatus 解析交易状态
func (b *Binance) parseTransactionStatus(status int) string {
	statusMap := map[int]string{
		0: "pending", // Email Sent
		1: "pending", // Cancelled
		2: "pending", // Awaiting Approval
		3: "pending", // Rejected
		4: "pending", // Processing
		5: "failed",  // Failure
		6: "ok",      // Completed
	}

	if mappedStatus, exists := statusMap[status]; exists {
		return mappedStatus
	}
	return "pending"
}

// FetchTradingFees 获取交易费率
func (b *Binance) FetchTradingFees(ctx context.Context, params map[string]interface{}) (map[string]*ccxt.TradingFee, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	response, err := b.PrivateAPI(ctx, "assetTradeFee", params)
	if err != nil {
		return nil, err
	}

	var feesData []map[string]interface{}
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &feesData); err != nil {
		return nil, err
	}

	fees := make(map[string]*ccxt.TradingFee)
	for _, feeData := range feesData {
		symbol := b.SafeString(feeData, "symbol", "")
		if symbol != "" {
			fees[symbol] = &ccxt.TradingFee{
				Symbol: symbol,
				Maker:  b.SafeFloat(feeData, "makerCommission", 0),
				Taker:  b.SafeFloat(feeData, "takerCommission", 0),
				Info:   feeData,
			}
		}
	}

	return fees, nil
}

// ========== 期货相关方法 ==========

// FetchPositions 获取持仓信息
func (b *Binance) FetchPositions(ctx context.Context, symbols []string, params map[string]interface{}) ([]*ccxt.Position, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	response, err := b.PrivateAPI(ctx, "futuresPositionRisk", params)
	if err != nil {
		return nil, err
	}

	var positionsData []FuturesPositionRisk
	responseBytes, _ := json.Marshal(response)
	if err := json.Unmarshal(responseBytes, &positionsData); err != nil {
		return nil, err
	}

	var positions []*ccxt.Position
	for _, positionData := range positionsData {
		position := b.parsePosition(&positionData)
		if position != nil {
			// 如果指定了symbols，则只返回匹配的持仓
			if len(symbols) == 0 || b.symbolInList(position.Symbol, symbols) {
				positions = append(positions, position)
			}
		}
	}

	return positions, nil
}

// SetLeverage 设置杠杆
func (b *Binance) SetLeverage(ctx context.Context, leverage int, symbol string, params map[string]interface{}) (*ccxt.LeverageInfo, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("market %s not found", symbol))
	}

	leverageParams := map[string]interface{}{
		"symbol":   market.ID,
		"leverage": leverage,
	}

	// 添加自定义参数
	for k, v := range params {
		leverageParams[k] = v
	}

	response, err := b.PrivateAPI(ctx, "futuresLeverage", leverageParams)
	if err != nil {
		return nil, err
	}

	responseMap := response.(map[string]interface{})
	return &ccxt.LeverageInfo{
		Symbol:   symbol,
		Leverage: leverage,
		Info:     responseMap,
	}, nil
}

// ========== 错误处理和重试机制 ==========

// HandleErrors 处理 Binance API 错误
func (b *Binance) HandleErrors(httpCode int, httpReason, url, method, headers, body string, response interface{}, requestHeaders, requestBody string) error {
	if httpCode == 200 {
		return nil
	}

	// 解析错误响应
	var errorResp map[string]interface{}
	if err := json.Unmarshal([]byte(body), &errorResp); err != nil {
		// 如果无法解析 JSON，返回基础 HTTP 错误
		return b.createHTTPError(httpCode, httpReason, body)
	}

	// 获取错误代码和消息
	code := b.SafeInt(errorResp, "code", int64(httpCode))
	msg := b.SafeString(errorResp, "msg", httpReason)

	// 根据错误代码返回特定错误类型
	return b.mapBinanceError(int(code), msg, body)
}

// mapBinanceError 映射 Binance 特定的错误代码
func (b *Binance) mapBinanceError(code int, message, body string) error {
	switch code {
	// 认证错误
	case -2014, -2015: // API-key format invalid / Invalid API-key
		return ccxt.NewAuthenticationError(fmt.Sprintf("Binance authentication error: %s", message))

		// 权限错误和余额不足 (Binance使用相同的错误代码)
	case -2010: // NEW_ORDER_REJECTED / Account has insufficient balance
		if strings.Contains(message, "insufficient") || strings.Contains(message, "balance") {
			return ccxt.NewInsufficientFunds("USDT", 0, 0) // 简化版本，实际需要解析具体数值
		}
		return ccxt.NewInvalidOrder(fmt.Sprintf("Binance order rejected: %s", message), "")

	// 限流错误
	case -1003: // Too many requests
		return ccxt.NewRateLimitExceeded(fmt.Sprintf("Binance rate limit exceeded: %s", message), 60)

	// 交易对不存在
	case -1121: // Invalid symbol
		return ccxt.NewMarketNotFound(fmt.Sprintf("Binance market not found: %s", message))

	// 网络错误
	case -1001: // Internal error
		return ccxt.NewNetworkError(fmt.Sprintf("Binance internal error: %s", message))

	// 交易所维护
	case -1002: // Unauthorized
		return ccxt.NewExchangeNotAvailable(fmt.Sprintf("Binance exchange not available: %s", message))

	// 参数错误
	case -1100, -1101, -1102, -1103, -1104, -1105, -1106: // Parameter errors
		return ccxt.NewBadRequest(fmt.Sprintf("Binance bad request: %s", message))

	// 订单不存在
	case -2013: // Order does not exist
		return ccxt.NewOrderNotFound(fmt.Sprintf("Binance order not found: %s", message))

	// 默认错误
	default:
		// HTTP 4xx 错误
		if code >= 400 && code < 500 {
			return ccxt.NewBadRequest(fmt.Sprintf("Binance bad request [%d]: %s", code, message))
		}
		// HTTP 5xx 错误
		if code >= 500 {
			return ccxt.NewExchangeError(fmt.Sprintf("Binance server error [%d]: %s", code, message))
		}
		// 其他错误
		return ccxt.NewExchangeError(fmt.Sprintf("Binance error [%d]: %s", code, message))
	}
}

// createHTTPError 创建 HTTP 错误
func (b *Binance) createHTTPError(httpCode int, httpReason, body string) error {
	switch httpCode {
	case 400:
		return ccxt.NewBadRequest(fmt.Sprintf("Bad Request: %s", body))
	case 401:
		return ccxt.NewAuthenticationError(fmt.Sprintf("Unauthorized: %s", body))
	case 403:
		return ccxt.NewPermissionDenied(fmt.Sprintf("Forbidden: %s", body))
	case 404:
		return ccxt.NewMarketNotFound(fmt.Sprintf("Not Found: %s", body))
	case 429:
		return ccxt.NewRateLimitExceeded(fmt.Sprintf("Rate Limit Exceeded: %s", body), 60)
	case 500, 502, 503, 504:
		return ccxt.NewExchangeNotAvailable(fmt.Sprintf("Server Error [%d]: %s", httpCode, body))
	default:
		return ccxt.NewNetworkError(fmt.Sprintf("HTTP Error [%d]: %s", httpCode, body))
	}
}

// RetryWithBackoff 带退避的重试机制
func (b *Binance) RetryWithBackoff(ctx context.Context, operation func() error, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// 指数退避：2^attempt * 100ms
			backoffDuration := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
			if backoffDuration > 10*time.Second {
				backoffDuration = 10 * time.Second // 最大退避时间
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDuration):
				// 继续重试
			}
		}

		lastErr = operation()
		if lastErr == nil {
			return nil // 成功
		}

		// 检查是否应该重试
		if !b.shouldRetry(lastErr) {
			return lastErr // 不应重试的错误，直接返回
		}
	}

	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}

// shouldRetry 判断错误是否应该重试
func (b *Binance) shouldRetry(err error) bool {
	// 网络错误，应该重试
	if _, ok := err.(*ccxt.NetworkError); ok {
		return true
	}

	// 交易所不可用，应该重试
	if _, ok := err.(*ccxt.ExchangeNotAvailable); ok {
		return true
	}

	// 限流错误，应该重试
	if _, ok := err.(*ccxt.RateLimitExceeded); ok {
		return true
	}

	// 其他错误不重试
	return false
}

// CalculateRateLimiterCost 计算 API 调用成本
func (b *Binance) CalculateRateLimiterCost(api, method, path string, params map[string]interface{}, config map[string]interface{}) int {
	// Binance API 权重配置
	weights := map[string]int{
		// 市场数据 API (public)
		"/api/v3/ping":              1,
		"/api/v3/time":              1,
		"/api/v3/exchangeInfo":      10,
		"/api/v3/ticker/24hr":       1, // 单个交易对
		"/api/v3/ticker/price":      1, // 单个交易对
		"/api/v3/depth":             1,
		"/api/v3/trades":            1,
		"/api/v3/historicalTrades":  5,
		"/api/v3/aggTrades":         1,
		"/api/v3/klines":            1,
		"/api/v3/ticker/bookTicker": 1,

		// 账户 API (private)
		"/api/v3/account":    10,
		"/api/v3/order":      1, // GET
		"/api/v3/allOrders":  10,
		"/api/v3/openOrders": 3,
		"/api/v3/myTrades":   10,

		// 交易 API (private)
		"/api/v3/order/test": 1, // 测试下单

		// 资金 API (private)
		"/sapi/v1/capital/deposit/address":  10,
		"/sapi/v1/capital/deposit/hisrec":   1,
		"/sapi/v1/capital/withdraw/history": 18000, // 注意：这个权重很高
		"/sapi/v1/capital/withdraw/apply":   600,

		// WebSocket 相关
		"/api/v3/userDataStream": 1,
	}

	// 获取基础权重
	weight := weights[path]
	if weight == 0 {
		weight = 1 // 默认权重
	}

	// 根据参数调整权重
	if path == "/api/v3/ticker/24hr" {
		// 如果没有指定 symbol，获取所有交易对的 ticker
		if symbol, exists := params["symbol"]; !exists || symbol == "" {
			weight = 40 // 获取所有交易对权重更高
		}
	}

	if path == "/api/v3/ticker/price" {
		// 如果没有指定 symbol，获取所有交易对的价格
		if symbol, exists := params["symbol"]; !exists || symbol == "" {
			weight = 2 // 获取所有交易对权重更高
		}
	}

	if path == "/api/v3/depth" {
		// 根据 limit 调整权重
		if limitParam, exists := params["limit"]; exists {
			if limit, ok := limitParam.(int); ok {
				if limit <= 100 {
					weight = 1
				} else if limit <= 500 {
					weight = 5
				} else if limit <= 1000 {
					weight = 10
				} else {
					weight = 50
				}
			}
		}
	}

	// POST 请求通常权重更高
	if method == "POST" && api == "private" {
		weight = weight * 2
	}

	return weight
}

// CheckRateLimit 检查和等待限流
func (b *Binance) CheckRateLimit(ctx context.Context, cost int) error {
	// 如果未启用限流，直接返回
	if !b.config.EnableRateLimit {
		return nil
	}

	// Binance 限流：1200 requests per minute (20 requests per second)
	rateLimit := 1200 // 默认限流

	// 计算需要等待的时间
	interval := time.Duration(60*1000/rateLimit) * time.Millisecond
	waitTime := time.Duration(cost) * interval

	if waitTime > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			return nil
		}
	}

	return nil
}

// HandleNetworkError 处理网络错误
func (b *Binance) HandleNetworkError(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()

	// 连接超时
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded") {
		return ccxt.NewRequestTimeout(fmt.Sprintf("Binance request timeout: %s", errStr))
	}

	// DNS 解析错误
	if strings.Contains(errStr, "no such host") {
		return ccxt.NewNetworkError(fmt.Sprintf("Binance DNS error: %s", errStr))
	}

	// 连接被拒绝
	if strings.Contains(errStr, "connection refused") {
		return ccxt.NewNetworkError(fmt.Sprintf("Binance connection refused: %s", errStr))
	}

	// SSL/TLS 错误
	if strings.Contains(errStr, "tls") || strings.Contains(errStr, "certificate") {
		return ccxt.NewNetworkError(fmt.Sprintf("Binance SSL error: %s", errStr))
	}

	// 默认网络错误
	return ccxt.NewNetworkError(fmt.Sprintf("Binance network error: %s", errStr))
}

// ========== 辅助方法 ==========

// convertParams 转换参数格式
func (b *Binance) convertParams(params map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range params {
		if v != nil {
			result[k] = fmt.Sprintf("%v", v)
		}
	}
	return result
}

// loadMarketsIfNeeded 如果需要则加载市场
func (b *Binance) loadMarketsIfNeeded(ctx context.Context) error {
	if len(b.BaseExchange.GetMarkets()) == 0 {
		_, err := b.LoadMarkets(ctx, false)
		return err
	}
	return nil
}

// fetchMarginBalance 获取保证金余额
func (b *Binance) fetchMarginBalance(ctx context.Context, params map[string]interface{}) (*ccxt.Account, error) {
	// 暂时返回现货余额，实际项目中应该调用保证金API
	return b.FetchBalance(ctx, params)
}

// fetchFuturesBalance 获取期货余额
func (b *Binance) fetchFuturesBalance(ctx context.Context, params map[string]interface{}) (*ccxt.Account, error) {
	// 暂时返回现货余额，实际项目中应该调用期货API
	return b.FetchBalance(ctx, params)
}

// getMarket 获取市场信息
func (b *Binance) getMarket(symbol string) (*ccxt.Market, error) {
	markets := b.BaseExchange.GetMarkets()
	if market, exists := markets[symbol]; exists {
		return market, nil
	}
	return nil, ccxt.NewMarketNotFound(symbol)
}

// getMarketByID 通过ID获取市场信息
func (b *Binance) getMarketByID(id string) *ccxt.Market {
	markets := b.BaseExchange.GetMarkets()
	for _, market := range markets {
		if market.ID == id {
			return market
		}
	}
	return nil
}

// getMarketType 获取市场类型

// getMarketType 获取市场类型
func (b *Binance) getMarketType(params map[string]interface{}) string {
	if marketType, ok := params["type"].(string); ok {
		return marketType
	}
	return b.config.DefaultType
}

// getOrderEndpoint 获取订单端点
func (b *Binance) getOrderEndpoint(marketType string) string {
	switch marketType {
	case "margin":
		return "marginOrder"
	case "future":
		return "futuresOrder"
	default:
		return "order"
	}
}

// getOpenOrdersEndpoint 获取开放订单端点
func (b *Binance) getOpenOrdersEndpoint(marketType string) string {
	switch marketType {
	case "margin":
		return "marginOpenOrders"
	case "future":
		return "futuresOpenOrders"
	default:
		return "openOrders"
	}
}

// PrivateAPI 私有API调用
func (b *Binance) PrivateAPI(ctx context.Context, method string, params map[string]interface{}) (interface{}, error) {
	if params == nil {
		params = make(map[string]interface{})
	}

	url, exists := b.endpoints[method]
	if !exists {
		return nil, ccxt.NewExchangeError(fmt.Sprintf("unknown endpoint: %s", method))
	}

	// 使用Sign方法进行签名
	signedURL, headers, body, err := b.Sign(url, "private", method, params, nil, nil)
	if err != nil {
		return nil, err
	}

	response, err := b.FetchWithRetry(ctx, signedURL, method, headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var result interface{}
	if err := json.Unmarshal([]byte(response), &result); err != nil {
		return nil, err
	}

	return result, nil
}

// ========== 生命周期管理 ==========

// Close 关闭交易所连接
func (b *Binance) Close() error {
	// 关闭WebSocket连接

	// 调用基类的Close方法
	return b.BaseExchange.Close()
}

// buildRequestURL 构建带参数的请求URL
func (b *Binance) buildRequestURL(endpoint string, params map[string]interface{}) string {
	if len(params) == 0 {
		return endpoint
	}

	values := url.Values{}
	for k, v := range params {
		values.Set(k, fmt.Sprintf("%v", v))
	}

	if strings.Contains(endpoint, "?") {
		return endpoint + "&" + values.Encode()
	}
	return endpoint + "?" + values.Encode()
}

// httpGet 发送GET请求
func (b *Binance) httpGet(ctx context.Context, endpoint string, params map[string]interface{}) (string, error) {
	url := b.buildRequestURL(endpoint, params)
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	return b.FetchWithRetry(ctx, url, "GET", headers, "")
}

// httpPost 发送POST请求
func (b *Binance) httpPost(ctx context.Context, endpoint string, params map[string]interface{}) (string, error) {
	body := ""
	if len(params) > 0 {
		jsonData, err := json.Marshal(params)
		if err != nil {
			return "", err
		}
		body = string(jsonData)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
	}
	return b.FetchWithRetry(ctx, endpoint, "POST", headers, body)
}

// SafeInt 安全获取整数值
func (b *Binance) SafeInt(data map[string]interface{}, key string, defaultValue int64) int64 {
	if value, exists := data[key]; exists {
		switch v := value.(type) {
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

// symbolInList 检查symbol是否在列表中
func (b *Binance) symbolInList(symbol string, symbols []string) bool {
	for _, s := range symbols {
		if s == symbol {
			return true
		}
	}
	return false
}

// ========== 状态和时间方法 ==========

// FetchStatus 获取交易所状态
func (b *Binance) FetchStatus(ctx context.Context, params map[string]interface{}) (*ccxt.Status, error) {
	// Binance 没有专门的状态端点，我们通过ping来检查状态
	_, err := b.httpGet(ctx, b.endpoints["ping"], nil)
	if err != nil {
		return &ccxt.Status{
			Status:  "error",
			Updated: b.Milliseconds(),
		}, nil
	}

	return &ccxt.Status{
		Status:  "ok",
		Updated: b.Milliseconds(),
	}, nil
}

// FetchTime 获取服务器时间
func (b *Binance) FetchTime(ctx context.Context, params map[string]interface{}) (int64, error) {
	response, err := b.httpGet(ctx, b.endpoints["time"], params)
	if err != nil {
		return 0, err
	}

	var timeData ServerTimeResponse
	if err := json.Unmarshal([]byte(response), &timeData); err != nil {
		return 0, err
	}

	return timeData.ServerTime, nil
}

// ========== 辅助工具方法 ==========

// ValidateRequiredParams 验证必需参数
func (b *Binance) ValidateRequiredParams(params map[string]interface{}, required []string) error {
	for _, param := range required {
		if value, exists := params[param]; !exists || value == nil || value == "" {
			return ccxt.NewBadRequest(fmt.Sprintf("Missing required parameter: %s", param))
		}
	}
	return nil
}

// ValidateOrderParams 验证订单参数
func (b *Binance) ValidateOrderParams(symbol, orderType, side string, amount, price float64) error {
	if symbol == "" {
		return ccxt.NewBadRequest("symbol is required")
	}

	if orderType == "" {
		return ccxt.NewBadRequest("orderType is required")
	}

	if side != "buy" && side != "sell" {
		return ccxt.NewBadRequest("side must be 'buy' or 'sell'")
	}

	if amount <= 0 {
		return ccxt.NewBadRequest("amount must be positive")
	}

	if (orderType == "limit" || orderType == "stop-limit") && price <= 0 {
		return ccxt.NewBadRequest("price must be positive for limit orders")
	}

	return nil
}

// SanitizeParams 清理参数
func (b *Binance) SanitizeParams(params map[string]interface{}) map[string]interface{} {
	if params == nil {
		return make(map[string]interface{})
	}

	sanitized := make(map[string]interface{})

	for key, value := range params {
		// 移除空值
		if value == nil || value == "" {
			continue
		}

		// 转换布尔值为字符串
		if boolVal, ok := value.(bool); ok {
			if boolVal {
				sanitized[key] = "true"
			} else {
				sanitized[key] = "false"
			}
			continue
		}

		sanitized[key] = value
	}

	return sanitized
}

// GetOrderStatus 标准化订单状态
func (b *Binance) GetOrderStatus(binanceStatus string) string {
	statusMap := map[string]string{
		"NEW":              "open",
		"PARTIALLY_FILLED": "open",
		"FILLED":           "closed",
		"CANCELED":         "canceled",
		"PENDING_CANCEL":   "canceling",
		"REJECTED":         "rejected",
		"EXPIRED":          "expired",
	}

	if status, exists := statusMap[binanceStatus]; exists {
		return status
	}

	return strings.ToLower(binanceStatus)
}

// GetOrderType 标准化订单类型
func (b *Binance) GetOrderType(binanceType string) string {
	typeMap := map[string]string{
		"LIMIT":             "limit",
		"MARKET":            "market",
		"STOP_LOSS":         "stop",
		"STOP_LOSS_LIMIT":   "stop-limit",
		"TAKE_PROFIT":       "take-profit",
		"TAKE_PROFIT_LIMIT": "take-profit-limit",
		"LIMIT_MAKER":       "limit",
	}

	if orderType, exists := typeMap[binanceType]; exists {
		return orderType
	}

	return strings.ToLower(binanceType)
}

// ========== 期货交易扩展 API ==========

// FetchLeverage 获取杠杆倍数
func (b *Binance) FetchLeverage(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.Leverage, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	leverageParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 添加自定义参数
	for k, v := range params {
		leverageParams[k] = v
	}

	url := "/fapi/v2/account"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", leverageParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var accountInfo struct {
		Positions []struct {
			Symbol       string `json:"symbol"`
			Leverage     string `json:"leverage"`
			PositionSide string `json:"positionSide"`
		} `json:"positions"`
	}
	if err := json.Unmarshal([]byte(respBody), &accountInfo); err != nil {
		return nil, err
	}

	// 查找指定交易对的杠杆信息
	for _, position := range accountInfo.Positions {
		if position.Symbol == market.ID {
			leverage := b.SafeFloat(map[string]interface{}{"leverage": position.Leverage}, "leverage", 0)
			return &ccxt.Leverage{
				Symbol:   symbol,
				Leverage: leverage,
				Info:     map[string]interface{}{"symbol": position.Symbol, "leverage": position.Leverage},
			}, nil
		}
	}

	return nil, ccxt.NewMarketNotFound(fmt.Sprintf("leverage info for %s not found", symbol))
}

// SetMarginMode 设置保证金模式
func (b *Binance) SetMarginMode(ctx context.Context, marginMode, symbol string, params map[string]interface{}) (*ccxt.MarginMode, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	// 标准化保证金模式
	var marginType string
	switch marginMode {
	case "isolated":
		marginType = "ISOLATED"
	case "cross":
		marginType = "CROSSED"
	default:
		return nil, ccxt.NewBadRequest(fmt.Sprintf("invalid margin mode: %s, expected 'isolated' or 'cross'", marginMode))
	}

	marginParams := map[string]interface{}{
		"symbol":     market.ID,
		"marginType": marginType,
	}

	// 添加自定义参数
	for k, v := range params {
		marginParams[k] = v
	}

	url := "/fapi/v1/marginType"
	signedURL, headers, body, err := b.Sign(url, "private", "POST", marginParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	if response.Code != 200 && response.Code != 0 {
		return nil, ccxt.NewExchangeError(fmt.Sprintf("Failed to set margin mode: %s", response.Msg))
	}

	return &ccxt.MarginMode{
		Symbol:     symbol,
		MarginMode: marginMode,
		Info:       map[string]interface{}{"symbol": market.ID, "marginType": marginType},
	}, nil
}

// FetchMarginMode 获取保证金模式
func (b *Binance) FetchMarginMode(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.MarginMode, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	marginParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 添加自定义参数
	for k, v := range params {
		marginParams[k] = v
	}

	url := "/fapi/v2/account"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", marginParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var accountInfo struct {
		Positions []struct {
			Symbol       string `json:"symbol"`
			MarginType   string `json:"marginType"`
			PositionSide string `json:"positionSide"`
		} `json:"positions"`
	}
	if err := json.Unmarshal([]byte(respBody), &accountInfo); err != nil {
		return nil, err
	}

	// 查找指定交易对的保证金模式
	for _, position := range accountInfo.Positions {
		if position.Symbol == market.ID {
			var mode string
			switch position.MarginType {
			case "ISOLATED":
				mode = "isolated"
			case "CROSSED":
				mode = "cross"
			default:
				mode = strings.ToLower(position.MarginType)
			}

			return &ccxt.MarginMode{
				Symbol:     symbol,
				MarginMode: mode,
				Info:       map[string]interface{}{"symbol": position.Symbol, "marginType": position.MarginType},
			}, nil
		}
	}

	return nil, ccxt.NewMarketNotFound(fmt.Sprintf("margin mode for %s not found", symbol))
}

// findMarketByID 根据市场ID查找市场信息
func (b *Binance) findMarketByID(marketID string) *ccxt.Market {
	markets := b.GetMarkets()
	for _, market := range markets {
		if market.ID == marketID {
			return market
		}
	}
	return nil
}

// FetchFundingRate 获取资金费率
func (b *Binance) FetchFundingRate(ctx context.Context, symbol string, params map[string]interface{}) (*ccxt.FundingRate, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	fundingParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 添加自定义参数
	for k, v := range params {
		fundingParams[k] = v
	}

	url := "/fapi/v1/fundingRate"
	signedURL, headers, body, err := b.Sign(url, "public", "GET", fundingParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response []struct {
		Symbol      string `json:"symbol"`
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	if len(response) == 0 {
		return nil, ccxt.NewMarketNotFound(fmt.Sprintf("funding rate for %s not found", symbol))
	}

	latest := response[len(response)-1]
	fundingRate := b.SafeFloat(map[string]interface{}{"rate": latest.FundingRate}, "rate", 0)

	return &ccxt.FundingRate{
		Symbol:           symbol,
		FundingRate:      fundingRate,
		FundingTimestamp: latest.FundingTime,
		FundingDatetime:  b.ISO8601(latest.FundingTime),
		Timestamp:        latest.FundingTime,
		Datetime:         b.ISO8601(latest.FundingTime),
		Info:             map[string]interface{}{"symbol": latest.Symbol, "fundingRate": latest.FundingRate, "fundingTime": latest.FundingTime},
	}, nil
}

// FetchFundingRates 获取多个交易对的资金费率
func (b *Binance) FetchFundingRates(ctx context.Context, symbols []string, params map[string]interface{}) (map[string]*ccxt.FundingRate, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	fundingParams := make(map[string]interface{})

	// 添加自定义参数
	for k, v := range params {
		fundingParams[k] = v
	}

	url := "/fapi/v1/premiumIndex"
	signedURL, headers, body, err := b.Sign(url, "public", "GET", fundingParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response []struct {
		Symbol               string `json:"symbol"`
		MarkPrice            string `json:"markPrice"`
		IndexPrice           string `json:"indexPrice"`
		EstimatedSettlePrice string `json:"estimatedSettlePrice"`
		LastFundingRate      string `json:"lastFundingRate"`
		NextFundingTime      int64  `json:"nextFundingTime"`
		InterestRate         string `json:"interestRate"`
		Time                 int64  `json:"time"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	result := make(map[string]*ccxt.FundingRate)
	for _, item := range response {
		market := b.findMarketByID(item.Symbol)
		if market == nil {
			continue
		}

		// 如果指定了symbols过滤，进行过滤
		if len(symbols) > 0 && !b.symbolInList(market.Symbol, symbols) {
			continue
		}

		fundingRate := &ccxt.FundingRate{
			Symbol:               market.Symbol,
			MarkPrice:            b.SafeFloat(map[string]interface{}{"price": item.MarkPrice}, "price", 0),
			IndexPrice:           b.SafeFloat(map[string]interface{}{"price": item.IndexPrice}, "price", 0),
			EstimatedSettlePrice: b.SafeFloat(map[string]interface{}{"price": item.EstimatedSettlePrice}, "price", 0),
			FundingRate:          b.SafeFloat(map[string]interface{}{"rate": item.LastFundingRate}, "rate", 0),
			InterestRate:         b.SafeFloat(map[string]interface{}{"rate": item.InterestRate}, "rate", 0),
			NextFundingTimestamp: item.NextFundingTime,
			NextFundingDatetime:  b.ISO8601(item.NextFundingTime),
			Timestamp:            item.Time,
			Datetime:             b.ISO8601(item.Time),
			Info:                 map[string]interface{}{"symbol": item.Symbol, "markPrice": item.MarkPrice, "lastFundingRate": item.LastFundingRate, "nextFundingTime": item.NextFundingTime},
		}

		result[market.Symbol] = fundingRate
	}

	return result, nil
}

// FetchFundingHistory 获取资金费率历史
func (b *Binance) FetchFundingHistory(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) ([]ccxt.FundingRate, error) {
	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	fundingParams := map[string]interface{}{
		"symbol": market.ID,
	}

	if since > 0 {
		fundingParams["startTime"] = since
	}

	if limit > 0 {
		fundingParams["limit"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		fundingParams[k] = v
	}

	url := "/fapi/v1/fundingRate"
	signedURL, headers, body, err := b.Sign(url, "public", "GET", fundingParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response []struct {
		Symbol      string `json:"symbol"`
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	var result []ccxt.FundingRate
	for _, item := range response {
		fundingRate := ccxt.FundingRate{
			Symbol:           symbol,
			FundingRate:      b.SafeFloat(map[string]interface{}{"rate": item.FundingRate}, "rate", 0),
			FundingTimestamp: item.FundingTime,
			FundingDatetime:  b.ISO8601(item.FundingTime),
			Timestamp:        item.FundingTime,
			Datetime:         b.ISO8601(item.FundingTime),
			Info:             map[string]interface{}{"symbol": item.Symbol, "fundingRate": item.FundingRate, "fundingTime": item.FundingTime},
		}
		result = append(result, fundingRate)
	}

	return result, nil
}

// ========== 保证金交易 API ==========

// BorrowMargin 保证金借贷
func (b *Binance) BorrowMargin(ctx context.Context, code string, amount float64, symbol string, params map[string]interface{}) (*ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	borrowParams := map[string]interface{}{
		"asset":  code,
		"amount": amount,
	}

	// 如果指定了symbol，添加到参数中
	if symbol != "" {
		market, err := b.getMarket(symbol)
		if err != nil {
			return nil, err
		}
		if market != nil {
			borrowParams["symbol"] = market.ID
		}
	}

	// 添加自定义参数
	for k, v := range params {
		borrowParams[k] = v
	}

	url := "/sapi/v1/margin/loan"
	signedURL, headers, body, err := b.Sign(url, "private", "POST", borrowParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		TranID int64 `json:"tranId"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	return &ccxt.Transaction{
		ID:        fmt.Sprintf("%d", response.TranID),
		Currency:  code,
		Amount:    amount,
		Type:      "margin-borrow",
		Status:    "ok",
		Timestamp: b.Milliseconds(),
		Datetime:  b.ISO8601(b.Milliseconds()),
		Info:      map[string]interface{}{"tranId": response.TranID},
	}, nil
}

// RepayMargin 保证金还款
func (b *Binance) RepayMargin(ctx context.Context, code string, amount float64, symbol string, params map[string]interface{}) (*ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	repayParams := map[string]interface{}{
		"asset":  code,
		"amount": amount,
	}

	// 如果指定了symbol，添加到参数中
	if symbol != "" {
		market, err := b.getMarket(symbol)
		if err != nil {
			return nil, err
		}
		if market != nil {
			repayParams["symbol"] = market.ID
		}
	}

	// 添加自定义参数
	for k, v := range params {
		repayParams[k] = v
	}

	url := "/sapi/v1/margin/repay"
	signedURL, headers, body, err := b.Sign(url, "private", "POST", repayParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		TranID int64 `json:"tranId"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	return &ccxt.Transaction{
		ID:        fmt.Sprintf("%d", response.TranID),
		Currency:  code,
		Amount:    amount,
		Type:      "margin-repay",
		Status:    "ok",
		Timestamp: b.Milliseconds(),
		Datetime:  b.ISO8601(b.Milliseconds()),
		Info:      map[string]interface{}{"tranId": response.TranID},
	}, nil
}

// FetchBorrowInterest 查询借贷利息
func (b *Binance) FetchBorrowInterest(ctx context.Context, code string, symbol string, since int64, limit int, params map[string]interface{}) ([]ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	interestParams := make(map[string]interface{})

	if code != "" {
		interestParams["asset"] = code
	}

	if since > 0 {
		interestParams["startTime"] = since
	}

	if limit > 0 {
		interestParams["size"] = limit
	}

	// 添加自定义参数
	for k, v := range params {
		interestParams[k] = v
	}

	url := "/sapi/v1/margin/interestHistory"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", interestParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		Rows []struct {
			TxID            int64  `json:"txId"`
			InterestAccrued string `json:"interestAccruedTime"`
			Asset           string `json:"asset"`
			RawAsset        string `json:"rawAsset"`
			Principal       string `json:"principal"`
			Interest        string `json:"interest"`
			InterestRate    string `json:"interestRate"`
			Type            string `json:"type"`
			IsolatedSymbol  string `json:"isolatedSymbol,omitempty"`
		} `json:"rows"`
		Total int `json:"total"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	var result []ccxt.Transaction
	for _, row := range response.Rows {
		timestamp, _ := strconv.ParseInt(row.InterestAccrued, 10, 64)
		interest := b.SafeFloat(map[string]interface{}{"interest": row.Interest}, "interest", 0)

		transaction := ccxt.Transaction{
			ID:        fmt.Sprintf("%d", row.TxID),
			Currency:  row.Asset,
			Amount:    interest,
			Type:      "margin-interest",
			Status:    "ok",
			Timestamp: timestamp,
			Datetime:  b.ISO8601(timestamp),
			Info: map[string]interface{}{
				"txId":           row.TxID,
				"asset":          row.Asset,
				"principal":      row.Principal,
				"interest":       row.Interest,
				"interestRate":   row.InterestRate,
				"type":           row.Type,
				"isolatedSymbol": row.IsolatedSymbol,
			},
		}
		result = append(result, transaction)
	}

	return result, nil
}

// FetchMarginBalance 获取保证金账户余额
func (b *Binance) FetchMarginBalance(ctx context.Context, params map[string]interface{}) (*ccxt.Account, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	marginParams := make(map[string]interface{})

	// 添加自定义参数
	for k, v := range params {
		marginParams[k] = v
	}

	url := "/sapi/v1/margin/account"
	signedURL, headers, body, err := b.Sign(url, "private", "GET", marginParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "GET", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		BorrowEnabled       bool   `json:"borrowEnabled"`
		MarginLevel         string `json:"marginLevel"`
		TotalAssetOfBtc     string `json:"totalAssetOfBtc"`
		TotalLiabilityOfBtc string `json:"totalLiabilityOfBtc"`
		TotalNetAssetOfBtc  string `json:"totalNetAssetOfBtc"`
		TradeEnabled        bool   `json:"tradeEnabled"`
		TransferEnabled     bool   `json:"transferEnabled"`
		UserAssets          []struct {
			Asset    string `json:"asset"`
			Borrowed string `json:"borrowed"`
			Free     string `json:"free"`
			Interest string `json:"interest"`
			Locked   string `json:"locked"`
			NetAsset string `json:"netAsset"`
		} `json:"userAssets"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	balances := make(map[string]ccxt.Balance)
	free := make(map[string]float64)
	used := make(map[string]float64)
	total := make(map[string]float64)

	for _, asset := range response.UserAssets {
		freeAmount := b.SafeFloat(map[string]interface{}{"free": asset.Free}, "free", 0)
		lockedAmount := b.SafeFloat(map[string]interface{}{"locked": asset.Locked}, "locked", 0)
		borrowedAmount := b.SafeFloat(map[string]interface{}{"borrowed": asset.Borrowed}, "borrowed", 0)
		interestAmount := b.SafeFloat(map[string]interface{}{"interest": asset.Interest}, "interest", 0)

		totalAmount := freeAmount + lockedAmount
		usedAmount := lockedAmount + borrowedAmount + interestAmount

		balances[asset.Asset] = ccxt.Balance{
			Free:  freeAmount,
			Used:  usedAmount,
			Total: totalAmount,
		}

		free[asset.Asset] = freeAmount
		used[asset.Asset] = usedAmount
		total[asset.Asset] = totalAmount
	}

	return &ccxt.Account{
		Type:     "margin",
		Balances: balances,
		Free:     free,
		Used:     used,
		Total:    total,
		Info: map[string]interface{}{
			"borrowEnabled":       response.BorrowEnabled,
			"marginLevel":         response.MarginLevel,
			"totalAssetOfBtc":     response.TotalAssetOfBtc,
			"totalLiabilityOfBtc": response.TotalLiabilityOfBtc,
			"totalNetAssetOfBtc":  response.TotalNetAssetOfBtc,
			"tradeEnabled":        response.TradeEnabled,
			"transferEnabled":     response.TransferEnabled,
		},
		Timestamp: b.Milliseconds(),
		Datetime:  b.ISO8601(b.Milliseconds()),
	}, nil
}

// TransferMargin 保证金账户转账
func (b *Binance) TransferMargin(ctx context.Context, code string, amount float64, fromAccount, toAccount string, params map[string]interface{}) (*ccxt.Transaction, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	// 转换账户类型
	var transferType int
	switch {
	case fromAccount == "spot" && toAccount == "margin":
		transferType = 1 // Main account to margin account
	case fromAccount == "margin" && toAccount == "spot":
		transferType = 2 // Margin account to main account
	default:
		return nil, ccxt.NewBadRequest(fmt.Sprintf("unsupported transfer direction: %s to %s", fromAccount, toAccount))
	}

	transferParams := map[string]interface{}{
		"asset":  code,
		"amount": amount,
		"type":   transferType,
	}

	// 添加自定义参数
	for k, v := range params {
		transferParams[k] = v
	}

	url := "/sapi/v1/margin/transfer"
	signedURL, headers, body, err := b.Sign(url, "private", "POST", transferParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		TranID int64 `json:"tranId"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	return &ccxt.Transaction{
		ID:          fmt.Sprintf("%d", response.TranID),
		Currency:    code,
		Amount:      amount,
		Type:        "transfer",
		Status:      "ok",
		AddressFrom: fromAccount,
		AddressTo:   toAccount,
		Timestamp:   b.Milliseconds(),
		Datetime:    b.ISO8601(b.Milliseconds()),
		Info:        map[string]interface{}{"tranId": response.TranID, "type": transferType},
	}, nil
}

// ========== 高级订单类型 API ==========

// CreateOCOOrder 创建 OCO (One-Cancels-Other) 订单
func (b *Binance) CreateOCOOrder(ctx context.Context, symbol, side string, amount, price, stopPrice, stopLimitPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	// 验证订单参数
	if err := b.ValidateOrderParams(symbol, "OCO", side, amount, price); err != nil {
		return nil, err
	}

	if stopPrice <= 0 {
		return nil, ccxt.NewBadRequest("stopPrice is required for OCO orders")
	}

	ocoParams := map[string]interface{}{
		"symbol":      market.ID,
		"side":        strings.ToUpper(side),
		"quantity":    amount,
		"price":       price,
		"stopPrice":   stopPrice,
		"timeInForce": "GTC", // Good Till Cancelled
	}

	// 如果指定了止损限价单价格
	if stopLimitPrice > 0 {
		ocoParams["stopLimitPrice"] = stopLimitPrice
		ocoParams["stopLimitTimeInForce"] = "GTC"
	}

	// 添加客户端订单ID
	if clientOrderID, exists := params["clientOrderId"]; exists {
		ocoParams["listClientOrderId"] = clientOrderID
	}

	// 添加限价单客户端订单ID
	if limitClientOrderID, exists := params["limitClientOrderId"]; exists {
		ocoParams["limitClientOrderId"] = limitClientOrderID
	}

	// 添加止损单客户端订单ID
	if stopClientOrderID, exists := params["stopClientOrderId"]; exists {
		ocoParams["stopClientOrderId"] = stopClientOrderID
	}

	// 添加其他自定义参数
	for k, v := range params {
		if k != "clientOrderId" && k != "limitClientOrderId" && k != "stopClientOrderId" {
			ocoParams[k] = v
		}
	}

	url := "/api/v3/order/oco"
	signedURL, headers, body, err := b.Sign(url, "private", "POST", ocoParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "POST", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		OrderListId       int64  `json:"orderListId"`
		ContingencyType   string `json:"contingencyType"`
		ListStatusType    string `json:"listStatusType"`
		ListOrderStatus   string `json:"listOrderStatus"`
		ListClientOrderId string `json:"listClientOrderId"`
		TransactionTime   int64  `json:"transactionTime"`
		Symbol            string `json:"symbol"`
		Orders            []struct {
			Symbol        string `json:"symbol"`
			OrderId       int64  `json:"orderId"`
			ClientOrderId string `json:"clientOrderId"`
		} `json:"orders"`
		OrderReports []struct {
			Symbol              string `json:"symbol"`
			OrderId             int64  `json:"orderId"`
			OrderListId         int64  `json:"orderListId"`
			ClientOrderId       string `json:"clientOrderId"`
			TransactTime        int64  `json:"transactTime"`
			Price               string `json:"price"`
			OrigQty             string `json:"origQty"`
			ExecutedQty         string `json:"executedQty"`
			CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
			Status              string `json:"status"`
			TimeInForce         string `json:"timeInForce"`
			Type                string `json:"type"`
			Side                string `json:"side"`
			StopPrice           string `json:"stopPrice,omitempty"`
		} `json:"orderReports"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	// 返回主订单（限价单）的信息
	var mainOrder *struct {
		Symbol              string `json:"symbol"`
		OrderId             int64  `json:"orderId"`
		OrderListId         int64  `json:"orderListId"`
		ClientOrderId       string `json:"clientOrderId"`
		TransactTime        int64  `json:"transactTime"`
		Price               string `json:"price"`
		OrigQty             string `json:"origQty"`
		ExecutedQty         string `json:"executedQty"`
		CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
		Status              string `json:"status"`
		TimeInForce         string `json:"timeInForce"`
		Type                string `json:"type"`
		Side                string `json:"side"`
		StopPrice           string `json:"stopPrice,omitempty"`
	}

	for i := range response.OrderReports {
		if response.OrderReports[i].Type == "LIMIT" {
			mainOrder = &response.OrderReports[i]
			break
		}
	}

	if mainOrder == nil && len(response.OrderReports) > 0 {
		mainOrder = &response.OrderReports[0]
	}

	if mainOrder == nil {
		return nil, ccxt.NewExchangeError("Failed to parse OCO order response")
	}

	orderPrice := b.SafeFloat(map[string]interface{}{"price": mainOrder.Price}, "price", 0)
	orderAmount := b.SafeFloat(map[string]interface{}{"amount": mainOrder.OrigQty}, "amount", 0)
	filled := b.SafeFloat(map[string]interface{}{"filled": mainOrder.ExecutedQty}, "filled", 0)
	cost := b.SafeFloat(map[string]interface{}{"cost": mainOrder.CummulativeQuoteQty}, "cost", 0)

	return &ccxt.Order{
		ID:                 fmt.Sprintf("%d", mainOrder.OrderId),
		ClientOrderId:      mainOrder.ClientOrderId,
		Timestamp:          mainOrder.TransactTime,
		Datetime:           b.ISO8601(mainOrder.TransactTime),
		LastTradeTimestamp: mainOrder.TransactTime,
		Symbol:             symbol,
		Type:               "OCO",
		TimeInForce:        strings.ToLower(mainOrder.TimeInForce),
		Side:               strings.ToLower(mainOrder.Side),
		Status:             b.GetOrderStatus(mainOrder.Status),
		Amount:             orderAmount,
		Price:              orderPrice,
		Cost:               cost,
		Average:            0,
		Filled:             filled,
		Remaining:          orderAmount - filled,
		Fee:                ccxt.Fee{},
		Trades:             nil,
		Info: map[string]interface{}{
			"orderListId":       response.OrderListId,
			"contingencyType":   response.ContingencyType,
			"listStatusType":    response.ListStatusType,
			"listOrderStatus":   response.ListOrderStatus,
			"listClientOrderId": response.ListClientOrderId,
			"orders":            response.Orders,
			"orderReports":      response.OrderReports,
		},
	}, nil
}

// CancelOCOOrder 取消 OCO 订单
func (b *Binance) CancelOCOOrder(ctx context.Context, id, symbol string, params map[string]interface{}) (*ccxt.Order, error) {
	if !b.config.RequiresAuth() {
		return nil, ccxt.NewAuthenticationError("API credentials required")
	}

	if err := b.loadMarketsIfNeeded(ctx); err != nil {
		return nil, err
	}

	market, err := b.getMarket(symbol)
	if err != nil {
		return nil, err
	}
	if market == nil {
		return nil, ccxt.NewMarketNotFound(symbol)
	}

	cancelParams := map[string]interface{}{
		"symbol": market.ID,
	}

	// 支持通过订单列表ID或客户端订单ID取消
	if listClientOrderID, exists := params["listClientOrderId"]; exists {
		cancelParams["listClientOrderId"] = listClientOrderID
	} else {
		// 默认使用订单列表ID
		cancelParams["orderListId"] = id
	}

	// 添加自定义参数
	for k, v := range params {
		cancelParams[k] = v
	}

	url := "/api/v3/orderList"
	signedURL, headers, body, err := b.Sign(url, "private", "DELETE", cancelParams, nil, nil)
	if err != nil {
		return nil, err
	}

	respBody, err := b.FetchWithRetry(ctx, signedURL, "DELETE", headers, fmt.Sprintf("%v", body))
	if err != nil {
		return nil, err
	}

	var response struct {
		OrderListId       int64  `json:"orderListId"`
		ContingencyType   string `json:"contingencyType"`
		ListStatusType    string `json:"listStatusType"`
		ListOrderStatus   string `json:"listOrderStatus"`
		ListClientOrderId string `json:"listClientOrderId"`
		TransactionTime   int64  `json:"transactionTime"`
		Symbol            string `json:"symbol"`
		OrderReports      []struct {
			Symbol              string `json:"symbol"`
			OrigClientOrderId   string `json:"origClientOrderId"`
			OrderId             int64  `json:"orderId"`
			OrderListId         int64  `json:"orderListId"`
			ClientOrderId       string `json:"clientOrderId"`
			Price               string `json:"price"`
			OrigQty             string `json:"origQty"`
			ExecutedQty         string `json:"executedQty"`
			CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
			Status              string `json:"status"`
			TimeInForce         string `json:"timeInForce"`
			Type                string `json:"type"`
			Side                string `json:"side"`
		} `json:"orderReports"`
	}
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, err
	}

	// 返回第一个订单的信息
	if len(response.OrderReports) == 0 {
		return nil, ccxt.NewOrderNotFound(fmt.Sprintf("OCO order %s not found", id))
	}

	mainOrder := response.OrderReports[0]
	orderAmount := b.SafeFloat(map[string]interface{}{"amount": mainOrder.OrigQty}, "amount", 0)
	filled := b.SafeFloat(map[string]interface{}{"filled": mainOrder.ExecutedQty}, "filled", 0)

	return &ccxt.Order{
		ID:                 fmt.Sprintf("%d", mainOrder.OrderId),
		ClientOrderId:      mainOrder.ClientOrderId,
		Timestamp:          response.TransactionTime,
		Datetime:           b.ISO8601(response.TransactionTime),
		LastTradeTimestamp: response.TransactionTime,
		Symbol:             symbol,
		Type:               "OCO",
		Side:               strings.ToLower(mainOrder.Side),
		Status:             "canceled",
		Amount:             orderAmount,
		Filled:             filled,
		Remaining:          orderAmount - filled,
		Info: map[string]interface{}{
			"orderListId":     response.OrderListId,
			"listStatusType":  response.ListStatusType,
			"listOrderStatus": response.ListOrderStatus,
			"orderReports":    response.OrderReports,
		},
	}, nil
}

// CreateStopLossOrder 创建止损订单
func (b *Binance) CreateStopLossOrder(ctx context.Context, symbol, side string, amount, stopPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}
	orderParams["stopPrice"] = stopPrice
	orderParams["type"] = "STOP_LOSS"

	return b.CreateOrder(ctx, symbol, "stop", side, amount, 0, orderParams)
}

// CreateStopLossLimitOrder 创建止损限价订单
func (b *Binance) CreateStopLossLimitOrder(ctx context.Context, symbol, side string, amount, price, stopPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}
	orderParams["stopPrice"] = stopPrice
	orderParams["type"] = "STOP_LOSS_LIMIT"

	return b.CreateOrder(ctx, symbol, "stop-limit", side, amount, price, orderParams)
}

// CreateTakeProfitOrder 创建止盈订单
func (b *Binance) CreateTakeProfitOrder(ctx context.Context, symbol, side string, amount, stopPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}
	orderParams["stopPrice"] = stopPrice
	orderParams["type"] = "TAKE_PROFIT"

	return b.CreateOrder(ctx, symbol, "take-profit", side, amount, 0, orderParams)
}

// CreateTakeProfitLimitOrder 创建止盈限价订单
func (b *Binance) CreateTakeProfitLimitOrder(ctx context.Context, symbol, side string, amount, price, stopPrice float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}
	orderParams["stopPrice"] = stopPrice
	orderParams["type"] = "TAKE_PROFIT_LIMIT"

	return b.CreateOrder(ctx, symbol, "take-profit-limit", side, amount, price, orderParams)
}

// CreateLimitMakerOrder 创建只做 Maker 限价订单
func (b *Binance) CreateLimitMakerOrder(ctx context.Context, symbol, side string, amount, price float64, params map[string]interface{}) (*ccxt.Order, error) {
	orderParams := make(map[string]interface{})
	for k, v := range params {
		orderParams[k] = v
	}
	orderParams["type"] = "LIMIT_MAKER"

	return b.CreateOrder(ctx, symbol, "limit", side, amount, price, orderParams)
}

// ========== 精度处理和验证 ==========

// AmountToPrecision 将数量格式化为交易对精度
func (b *Binance) AmountToPrecision(symbol string, amount float64) float64 {
	if err := b.loadMarketsIfNeeded(context.Background()); err != nil {
		return amount // 如果无法加载市场，返回原值
	}

	market, err := b.getMarket(symbol)
	if err != nil || market == nil {
		return amount
	}

	if market.Precision.Amount > 0 {
		return b.floatToPrecision(amount, int(market.Precision.Amount))
	}

	return amount
}

// PriceToPrecision 将价格格式化为交易对精度
func (b *Binance) PriceToPrecision(symbol string, price float64) float64 {
	if err := b.loadMarketsIfNeeded(context.Background()); err != nil {
		return price
	}

	market, err := b.getMarket(symbol)
	if err != nil || market == nil {
		return price
	}

	if market.Precision.Price > 0 {
		return b.floatToPrecision(price, int(market.Precision.Price))
	}

	return price
}

// CostToPrecision 将成本格式化为交易对精度
func (b *Binance) CostToPrecision(symbol string, cost float64) float64 {
	if err := b.loadMarketsIfNeeded(context.Background()); err != nil {
		return cost
	}

	market, err := b.getMarket(symbol)
	if err != nil || market == nil {
		return cost
	}

	if market.Precision.Cost > 0 {
		return b.floatToPrecision(cost, int(market.Precision.Cost))
	}

	return cost
}

// floatToPrecision 将浮点数格式化为指定精度
func (b *Binance) floatToPrecision(value float64, precision int) float64 {
	if precision <= 0 {
		return value
	}

	multiplier := 1.0
	for i := 0; i < precision; i++ {
		multiplier *= 10
	}

	return float64(int64(value*multiplier+0.5)) / multiplier
}

// ValidateMarketLimits 验证订单是否符合市场限制
func (b *Binance) ValidateMarketLimits(symbol string, amount, price, cost float64) error {
	if err := b.loadMarketsIfNeeded(context.Background()); err != nil {
		return err
	}

	market, err := b.getMarket(symbol)
	if err != nil || market == nil {
		return ccxt.NewMarketNotFound(symbol)
	}

	// 验证数量限制
	if market.Limits.Amount.Min > 0 && amount < market.Limits.Amount.Min {
		return ccxt.NewBadRequest(fmt.Sprintf("amount %.8f is below minimum %.8f for %s", amount, market.Limits.Amount.Min, symbol))
	}

	if market.Limits.Amount.Max > 0 && amount > market.Limits.Amount.Max {
		return ccxt.NewBadRequest(fmt.Sprintf("amount %.8f exceeds maximum %.8f for %s", amount, market.Limits.Amount.Max, symbol))
	}

	// 验证价格限制
	if price > 0 {
		if market.Limits.Price.Min > 0 && price < market.Limits.Price.Min {
			return ccxt.NewBadRequest(fmt.Sprintf("price %.8f is below minimum %.8f for %s", price, market.Limits.Price.Min, symbol))
		}

		if market.Limits.Price.Max > 0 && price > market.Limits.Price.Max {
			return ccxt.NewBadRequest(fmt.Sprintf("price %.8f exceeds maximum %.8f for %s", price, market.Limits.Price.Max, symbol))
		}
	}

	// 验证成本限制
	if cost > 0 {
		if market.Limits.Cost.Min > 0 && cost < market.Limits.Cost.Min {
			return ccxt.NewBadRequest(fmt.Sprintf("cost %.8f is below minimum %.8f for %s", cost, market.Limits.Cost.Min, symbol))
		}

		if market.Limits.Cost.Max > 0 && cost > market.Limits.Cost.Max {
			return ccxt.NewBadRequest(fmt.Sprintf("cost %.8f exceeds maximum %.8f for %s", cost, market.Limits.Cost.Max, symbol))
		}
	}

	return nil
}

// GetTradingFees 获取交易费率
func (b *Binance) GetTradingFees() map[string]map[string]interface{} {
	return map[string]map[string]interface{}{
		"trading": {
			"maker":      0.001, // 0.1%
			"taker":      0.001, // 0.1%
			"percentage": true,
			"tierBased":  true,
		},
		"funding": {
			"withdraw": map[string]interface{}{
				"BTC":  0.0005,
				"ETH":  0.005,
				"USDT": 1.0,
			},
			"deposit": map[string]interface{}{},
		},
	}
}

// ========== 性能优化方法 ==========

// GetMarketFromCache 从缓存获取市场信息
func (b *Binance) GetMarketFromCache(symbol string) *ccxt.Market {
	markets := b.GetMarkets()
	if market, exists := markets[symbol]; exists {
		return market
	}
	return nil
}

// BatchCreateOrders 批量创建订单（模拟实现，Binance 没有原生支持）
func (b *Binance) BatchCreateOrders(ctx context.Context, orders []map[string]interface{}) ([]ccxt.Order, error) {
	var results []ccxt.Order
	var errors []error

	for _, orderData := range orders {
		symbol, _ := orderData["symbol"].(string)
		orderType, _ := orderData["type"].(string)
		side, _ := orderData["side"].(string)
		amount, _ := orderData["amount"].(float64)
		price, _ := orderData["price"].(float64)
		params, _ := orderData["params"].(map[string]interface{})

		if params == nil {
			params = make(map[string]interface{})
		}

		order, err := b.CreateOrder(ctx, symbol, orderType, side, amount, price, params)
		if err != nil {
			errors = append(errors, err)
			// 创建一个错误占位符
			results = append(results, ccxt.Order{
				Symbol: symbol,
				Status: "failed",
				Info:   map[string]interface{}{"error": err.Error()},
			})
		} else {
			results = append(results, *order)
		}
	}

	// 如果有错误，返回第一个错误
	if len(errors) > 0 {
		return results, errors[0]
	}

	return results, nil
}

// CalculateOrderCost 计算订单成本
func (b *Binance) CalculateOrderCost(symbol, side string, amount, price float64) (float64, error) {
	if amount <= 0 {
		return 0, ccxt.NewBadRequest("amount must be positive")
	}

	if price <= 0 {
		return 0, ccxt.NewBadRequest("price must be positive")
	}

	cost := amount * price

	// 应用精度
	cost = b.CostToPrecision(symbol, cost)

	return cost, nil
}

// EstimateFee 估算交易费用
func (b *Binance) EstimateFee(symbol, side string, amount, price float64, orderType string) (ccxt.Fee, error) {
	cost, err := b.CalculateOrderCost(symbol, side, amount, price)
	if err != nil {
		return ccxt.Fee{}, err
	}

	var feeRate float64
	switch orderType {
	case "limit", "limit-maker":
		feeRate = 0.001 // 0.1% maker fee
	case "market":
		feeRate = 0.001 // 0.1% taker fee
	default:
		feeRate = 0.001 // 默认费率
	}

	feeCost := cost * feeRate

	// 获取费用币种
	market, err := b.getMarket(symbol)
	if err != nil || market == nil {
		return ccxt.Fee{}, ccxt.NewMarketNotFound(symbol)
	}

	var feeCurrency string
	if side == "buy" {
		feeCurrency = market.Base
	} else {
		feeCurrency = market.Quote
	}

	return ccxt.Fee{
		Currency: feeCurrency,
		Cost:     feeCost,
		Rate:     feeRate,
	}, nil
}

// ========== 工具方法增强 ==========

// FormatSymbol 标准化交易对格式
func (b *Binance) FormatSymbol(symbol string) string {
	// 移除所有分隔符并转为大写
	symbol = strings.ReplaceAll(symbol, "/", "")
	symbol = strings.ReplaceAll(symbol, "-", "")
	symbol = strings.ReplaceAll(symbol, "_", "")
	return strings.ToUpper(symbol)
}

// ParseSymbol 解析交易对为基础和报价货币
func (b *Binance) ParseSymbol(symbol string) (base, quote string, err error) {
	if err := b.loadMarketsIfNeeded(context.Background()); err != nil {
		return "", "", err
	}

	market, err := b.getMarket(symbol)
	if err != nil || market == nil {
		return "", "", ccxt.NewMarketNotFound(symbol)
	}

	return market.Base, market.Quote, nil
}

// GetServerTime 获取服务器时间（缓存版本）
func (b *Binance) GetServerTime(ctx context.Context) (int64, error) {
	// 简单缓存机制，避免频繁请求
	now := time.Now().UnixMilli()

	// 如果距离上次请求不到1秒，使用本地时间
	if b.lastServerTimeRequest != 0 && now-b.lastServerTimeRequest < 1000 {
		return now + b.serverTimeOffset, nil
	}

	serverTime, err := b.FetchTime(ctx, nil)
	if err != nil {
		return now, err // 降级到本地时间
	}

	b.lastServerTimeRequest = now
	b.serverTimeOffset = serverTime - now

	return serverTime, nil
}

// ========== 状态检查和健康监控 ==========

// GetExchangeStatus 获取交易所状态
func (b *Binance) GetExchangeStatus(ctx context.Context) (map[string]interface{}, error) {
	// 检查连接性
	_, err := b.FetchTime(ctx, nil)
	if err != nil {
		return map[string]interface{}{
			"status": "offline",
			"error":  err.Error(),
		}, err
	}

	return map[string]interface{}{
		"status":    "online",
		"exchange":  "binance",
		"version":   "v3",
		"timestamp": b.Milliseconds(),
		"rateLimit": b.config.EnableRateLimit,
		"sandbox":   b.config.Sandbox,
		"testnet":   b.config.TestNet,
	}, nil
}

// FetchWithRetry 发送带重试和限流的HTTP请求
func (b *Binance) FetchWithRetry(ctx context.Context, url, method string, headers map[string]string, body string) (string, error) {
	// 计算API调用成本
	path := b.extractPath(url)
	cost := b.CalculateRateLimiterCost("private", method, path, nil, nil)

	// 检查限流
	if err := b.CheckRateLimit(ctx, cost); err != nil {
		return "", err
	}

	// 调用基类的重试逻辑
	return b.BaseExchange.FetchWithRetry(ctx, url, method, headers, body)
}

// extractPath 从URL中提取路径
func (b *Binance) extractPath(fullURL string) string {
	// 简单实现：提取API路径
	if idx := strings.Index(fullURL, "/api/"); idx != -1 {
		if qIdx := strings.Index(fullURL[idx:], "?"); qIdx != -1 {
			return fullURL[idx : idx+qIdx]
		}
		return fullURL[idx:]
	}
	if idx := strings.Index(fullURL, "/sapi/"); idx != -1 {
		if qIdx := strings.Index(fullURL[idx:], "?"); qIdx != -1 {
			return fullURL[idx : idx+qIdx]
		}
		return fullURL[idx:]
	}
	return "/"
}

// ========== WebSocket 方法委托 ==========

// WatchTicker 观察单个交易对的ticker
func (b *Binance) WatchTicker(ctx context.Context, symbol string, params map[string]interface{}) (<-chan *ccxt.WatchTicker, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}
	return b.wsClient.WatchTicker(ctx, symbol, params)
}

// WatchTickers 观察多个交易对的ticker
func (b *Binance) WatchTickers(ctx context.Context, symbols []string, params map[string]interface{}) (<-chan map[string]*ccxt.WatchTicker, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}

	if len(symbols) == 0 {
		return nil, ccxt.NewInvalidRequest("symbols array cannot be empty")
	}

	// 确保WebSocket连接
	if !b.wsClient.isConnected {
		if err := b.wsClient.Connect(ctx); err != nil {
			return nil, fmt.Errorf("failed to connect websocket: %w", err)
		}
	}

	// 创建结果channel
	resultChan := make(chan map[string]*ccxt.WatchTicker, 1000)

	// 存储ticker数据
	tickerMap := sync.Map{} // 线程安全的map

	// 构建批量订阅流列表
	streams := make([]string, len(symbols))
	for i, symbol := range symbols {
		normalizedSymbol := strings.ToLower(b.wsClient.convertSymbol(symbol))
		streams[i] = fmt.Sprintf("%s@ticker", normalizedSymbol)
	}

	// 批量订阅ticker流
	if err := b.wsClient.SubscribeToStreams(streams); err != nil {
		return nil, fmt.Errorf("failed to subscribe to ticker streams: %w", err)
	}

	// 启动消息处理器
	go func() {
		defer close(resultChan)

		// 监听ticker频道
		for ticker := range b.wsClient.tickerChan {
			if ticker == nil {
				continue
			}

			// 检查symbol是否在订阅列表中
			normalizedSymbol := strings.ToLower(b.wsClient.convertSymbol(ticker.Symbol))
			found := false
			for _, sym := range symbols {
				if strings.ToLower(b.wsClient.convertSymbol(sym)) == normalizedSymbol {
					found = true
					break
				}
			}

			if !found {
				continue // 不是我们订阅的symbol
			}

			// 更新ticker数据
			tickerMap.Store(ticker.Symbol, ticker)

			// 构建当前所有ticker的快照
			currentTickers := make(map[string]*ccxt.WatchTicker)
			tickerMap.Range(func(key, value interface{}) bool {
				if symbol, ok := key.(string); ok {
					if tickerData, ok := value.(*ccxt.WatchTicker); ok {
						// 只包含请求的symbols
						for _, requestedSymbol := range symbols {
							if strings.EqualFold(symbol, requestedSymbol) {
								currentTickers[symbol] = tickerData
								break
							}
						}
					}
				}
				return true
			})

			// 只有当包含数据时才发送
			if len(currentTickers) > 0 {
				select {
				case resultChan <- currentTickers:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return resultChan, nil
}

// WatchOrderBook 观察订单簿
func (b *Binance) WatchOrderBook(ctx context.Context, symbol string, limit int, params map[string]interface{}) (<-chan *ccxt.WatchOrderBook, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}
	return b.wsClient.WatchOrderBook(ctx, symbol, limit, params)
}

// WatchTrades 观察交易数据
func (b *Binance) WatchTrades(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan *ccxt.WatchTrade, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}

	// 获取原始的交易数组channel
	tradesChan, err := b.wsClient.WatchTrades(ctx, symbol, since, limit, params)
	if err != nil {
		return nil, err
	}

	// 创建单个交易的channel来匹配接口
	singleTradeChan := make(chan *ccxt.WatchTrade, 1000)
	go func() {
		defer close(singleTradeChan)
		for trades := range tradesChan {
			for _, trade := range trades {
				singleTradeChan <- &trade
			}
		}
	}()

	return singleTradeChan, nil
}

// WatchOHLCV 观察K线数据
func (b *Binance) WatchOHLCV(ctx context.Context, symbol, timeframe string, since int64, limit int, params map[string]interface{}) (<-chan *ccxt.WatchOHLCV, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}

	// 获取原始的K线数组channel
	ohlcvsChan, err := b.wsClient.WatchOHLCV(ctx, symbol, timeframe, since, limit, params)
	if err != nil {
		return nil, err
	}

	// 创建单个K线的channel来匹配接口
	singleOHLCVChan := make(chan *ccxt.WatchOHLCV, 1000)
	go func() {
		defer close(singleOHLCVChan)
		for ohlcvs := range ohlcvsChan {
			for _, ohlcv := range ohlcvs {
				singleOHLCVChan <- &ohlcv
			}
		}
	}()

	return singleOHLCVChan, nil
}

// WatchBalance 观察账户余额
func (b *Binance) WatchBalance(ctx context.Context, params map[string]interface{}) (<-chan *ccxt.WatchBalance, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}
	return b.wsClient.WatchBalance(ctx, params)
}

// WatchOrders 观察订单状态
func (b *Binance) WatchOrders(ctx context.Context, symbol string, since int64, limit int, params map[string]interface{}) (<-chan *ccxt.WatchOrder, error) {
	if b.wsClient == nil {
		return nil, ccxt.NewNotSupported("WebSocket not enabled")
	}

	// 获取原始的订单数组channel
	ordersChan, err := b.wsClient.WatchOrders(ctx, symbol, since, limit, params)
	if err != nil {
		return nil, err
	}

	// 创建单个订单的channel来匹配接口
	singleOrderChan := make(chan *ccxt.WatchOrder, 1000)
	go func() {
		defer close(singleOrderChan)
		for orders := range ordersChan {
			for _, order := range orders {
				singleOrderChan <- &order
			}
		}
	}()

	return singleOrderChan, nil
}
