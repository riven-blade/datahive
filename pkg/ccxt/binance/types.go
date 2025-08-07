package binance

import (
	"encoding/json"
)

// ========== Binance WebSocket 事件类型常数 ==========

// Binance原生WebSocket事件类型
const (
	EventTypeTrade          = "trade"           // 交易事件
	EventType24hrTicker     = "24hrTicker"      // 24小时价格统计事件
	EventType24hrMiniTicker = "24hrMiniTicker"  // 24小时迷你价格统计事件
	EventTypeBookTicker     = "bookTicker"      // 最优挂单信息事件
	EventTypeMarkPrice      = "markPriceUpdate" // 标记价格更新事件（期货）
	EventTypeKline          = "kline"           // K线事件
	EventTypeDepthUpdate    = "depthUpdate"     // 深度更新事件
	EventTypeBalanceUpdate  = "balanceUpdate"   // 余额更新事件
)

// ========== Binance WebSocket 字段名称常量 ==========

// 通用字段
const (
	FieldEventType = "e" // 事件类型
	FieldEventTime = "E" // 事件时间
	FieldSymbol    = "s" // 交易对
)

// Ticker 相关字段
const (
	FieldOpen        = "o" // 开盘价
	FieldHigh        = "h" // 最高价
	FieldLow         = "l" // 最低价
	FieldClose       = "c" // 收盘价
	FieldVolume      = "v" // 成交量
	FieldQuoteVolume = "q" // 成交额
)

// 标记价格相关字段
const (
	FieldMarkPrice   = "p" // 标记价格 (markPriceUpdate 事件中是 "p")
	FieldIndexPrice  = "i" // 指数价格
	FieldFundingRate = "r" // 资金费率
	FieldFundingTime = "T" // 资金费时间
)

// BookTicker 相关字段
const (
	FieldUpdateId = "u" // 更新ID
	FieldBidPrice = "b" // 买价
	FieldBidQty   = "B" // 买量
	FieldAskPrice = "a" // 卖价
	FieldAskQty   = "A" // 卖量
)

// 交易相关字段
const (
	FieldTradeId   = "t" // 交易ID
	FieldPrice     = "p" // 价格
	FieldQuantity  = "q" // 数量
	FieldTradeTime = "T" // 交易时间
)

// K线相关字段
const (
	FieldKlineData      = "k" // K线数据
	FieldKlineStartTime = "t" // K线开始时间
	FieldKlineInterval  = "i" // K线间隔
)

// WebSocket 协议字段
const (
	FieldStream = "stream" // 流名称
	FieldData   = "data"   // 数据内容
	FieldResult = "result" // 响应结果
	FieldError  = "error"  // 错误信息
	FieldMethod = "method" // 方法名称
	FieldParams = "params" // 参数
	FieldId     = "id"     // 请求ID
)

// 参数字段常量
const (
	ParamStream = "stream_name" // 自定义流名称参数
)

// WebSocket 方法常量
const (
	MethodSubscribe   = "SUBSCRIBE"          // 订阅
	MethodUnsubscribe = "UNSUBSCRIBE"        // 取消订阅
	MethodListStreams = "LIST_SUBSCRIPTIONS" // 列出订阅
	MethodSetProperty = "SET_PROPERTY"       // 设置属性
	MethodGetProperty = "GET_PROPERTY"       // 获取属性
)

// WebSocket 流名称模板常量
const (
	StreamTemplateTrade        = "%s@trade"      // 交易流模板
	StreamTemplateTicker       = "%s@ticker"     // 24小时ticker流模板
	StreamTemplateMiniTicker   = "%s@miniTicker" // 迷你ticker流模板
	StreamTemplateBookTicker   = "%s@bookTicker" // 最优挂单流模板
	StreamTemplateMarkPrice    = "%s@markPrice"  // 标记价格流模板
	StreamTemplateDepth        = "%s@depth"      // 深度流模板
	StreamTemplateKline        = "%s@kline_%s"   // K线流模板 (symbol, interval)
	StreamTemplateKlineDefault = "%s@kline_1m"   // 默认K线流模板
)

// ========== 通用响应结构 ==========

// APIResponse Binance API 通用响应
type APIResponse struct {
	Code int    `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

// ServerTimeResponse 服务器时间响应
type ServerTimeResponse struct {
	ServerTime int64 `json:"serverTime"`
}

// ========== 市场数据相关类型 ==========

// ExchangeInfoResponse 交易所信息响应
type ExchangeInfoResponse struct {
	Timezone   string `json:"timezone"`
	ServerTime int64  `json:"serverTime"`
	RateLimits []struct {
		RateLimitType string `json:"rateLimitType"`
		Interval      string `json:"interval"`
		IntervalNum   int    `json:"intervalNum"`
		Limit         int    `json:"limit"`
	} `json:"rateLimits"`
	ExchangeFilters []interface{} `json:"exchangeFilters"`
	Symbols         []SymbolInfo  `json:"symbols"`
}

// SymbolInfo 交易对信息
type SymbolInfo struct {
	Symbol                          string                   `json:"symbol"`
	Status                          string                   `json:"status"`
	BaseAsset                       string                   `json:"baseAsset"`
	BaseAssetPrecision              int                      `json:"baseAssetPrecision"`
	QuoteAsset                      string                   `json:"quoteAsset"`
	QuotePrecision                  int                      `json:"quotePrecision"`
	QuoteAssetPrecision             int                      `json:"quoteAssetPrecision"`
	BaseCommissionPrecision         int                      `json:"baseCommissionPrecision"`
	QuoteCommissionPrecision        int                      `json:"quoteCommissionPrecision"`
	OrderTypes                      []string                 `json:"orderTypes"`
	IcebergAllowed                  bool                     `json:"icebergAllowed"`
	OcoAllowed                      bool                     `json:"ocoAllowed"`
	QuoteOrderQtyMarketAllowed      bool                     `json:"quoteOrderQtyMarketAllowed"`
	AllowTrailingStop               bool                     `json:"allowTrailingStop"`
	CancelReplaceAllowed            bool                     `json:"cancelReplaceAllowed"`
	IsSpotTradingAllowed            bool                     `json:"isSpotTradingAllowed"`
	IsMarginTradingAllowed          bool                     `json:"isMarginTradingAllowed"`
	Filters                         []map[string]interface{} `json:"filters"`
	Permissions                     []string                 `json:"permissions"`
	DefaultSelfTradePreventionMode  string                   `json:"defaultSelfTradePreventionMode"`
	AllowedSelfTradePreventionModes []string                 `json:"allowedSelfTradePreventionModes"`
}

// Ticker24HrResponse 24小时ticker响应
type Ticker24HrResponse struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	WeightedAvgPrice   string `json:"weightedAvgPrice"`
	PrevClosePrice     string `json:"prevClosePrice"`
	LastPrice          string `json:"lastPrice"`
	LastQty            string `json:"lastQty"`
	BidPrice           string `json:"bidPrice"`
	BidQty             string `json:"bidQty"`
	AskPrice           string `json:"askPrice"`
	AskQty             string `json:"askQty"`
	OpenPrice          string `json:"openPrice"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	OpenTime           int64  `json:"openTime"`
	CloseTime          int64  `json:"closeTime"`
	FirstId            int64  `json:"firstId"`
	LastId             int64  `json:"lastId"`
	Count              int64  `json:"count"`
}

// BookTickerResponse 最优订单簿价格
type BookTickerResponse struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bidPrice"`
	BidQty   string `json:"bidQty"`
	AskPrice string `json:"askPrice"`
	AskQty   string `json:"askQty"`
}

// KlineResponse K线数据响应 (数组格式)
type KlineResponse [][]interface{}

// TradeResponse 最近交易响应
type TradeResponse struct {
	ID           int64  `json:"id"`
	Price        string `json:"price"`
	Qty          string `json:"qty"`
	QuoteQty     string `json:"quoteQty"`
	Time         int64  `json:"time"`
	IsBuyerMaker bool   `json:"isBuyerMaker"`
	IsBestMatch  bool   `json:"isBestMatch"`
}

// AggTradeResponse 聚合交易响应
type AggTradeResponse struct {
	AggTradeId       int64  `json:"a"` // 聚合交易ID
	Price            string `json:"p"` // 价格
	Qty              string `json:"q"` // 数量
	FirstTradeId     int64  `json:"f"` // 被聚合的首个交易ID
	LastTradeId      int64  `json:"l"` // 被聚合的末个交易ID
	Timestamp        int64  `json:"T"` // 交易时间戳
	IsBuyerMaker     bool   `json:"m"` // 是否为主动卖出单
	IsBestPriceMatch bool   `json:"M"` // 是否为最优撮合单
}

// DepthResponse 深度信息响应
type DepthResponse struct {
	LastUpdateId int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// ========== 账户相关类型 ==========

// AccountResponse 账户信息响应
type AccountResponse struct {
	MakerCommission            int64            `json:"makerCommission"`
	TakerCommission            int64            `json:"takerCommission"`
	BuyerCommission            int64            `json:"buyerCommission"`
	SellerCommission           int64            `json:"sellerCommission"`
	CommissionRates            *CommissionRates `json:"commissionRates,omitempty"`
	CanTrade                   bool             `json:"canTrade"`
	CanWithdraw                bool             `json:"canWithdraw"`
	CanDeposit                 bool             `json:"canDeposit"`
	Brokered                   bool             `json:"brokered"`
	RequireSelfTradePrevention bool             `json:"requireSelfTradePrevention"`
	PreventSor                 bool             `json:"preventSor"`
	UpdateTime                 int64            `json:"updateTime"`
	AccountType                string           `json:"accountType"`
	Balances                   []BalanceInfo    `json:"balances"`
	Permissions                []string         `json:"permissions"`
	UID                        int64            `json:"uid"`
}

// CommissionRates 手续费率
type CommissionRates struct {
	Maker  string `json:"maker"`
	Taker  string `json:"taker"`
	Buyer  string `json:"buyer"`
	Seller string `json:"seller"`
}

// BalanceInfo 余额信息
type BalanceInfo struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

// ========== 订单相关类型 ==========

// OrderResponse 订单响应
type OrderResponse struct {
	Symbol                  string `json:"symbol"`
	OrderId                 int64  `json:"orderId"`
	OrderListId             int64  `json:"orderListId"`
	ClientOrderId           string `json:"clientOrderId"`
	Price                   string `json:"price"`
	OrigQty                 string `json:"origQty"`
	ExecutedQty             string `json:"executedQty"`
	CummulativeQuoteQty     string `json:"cummulativeQuoteQty"`
	Status                  string `json:"status"`
	TimeInForce             string `json:"timeInForce"`
	Type                    string `json:"type"`
	Side                    string `json:"side"`
	StopPrice               string `json:"stopPrice"`
	IcebergQty              string `json:"icebergQty"`
	Time                    int64  `json:"time"`
	UpdateTime              int64  `json:"updateTime"`
	IsWorking               bool   `json:"isWorking"`
	WorkingTime             int64  `json:"workingTime"`
	OrigQuoteOrderQty       string `json:"origQuoteOrderQty"`
	SelfTradePreventionMode string `json:"selfTradePreventionMode"`
	Fills                   []Fill `json:"fills,omitempty"`
}

// Fill 成交信息
type Fill struct {
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	TradeId         int64  `json:"tradeId"`
}

// MyTradeResponse 我的交易响应
type MyTradeResponse struct {
	Symbol          string `json:"symbol"`
	Id              int64  `json:"id"`
	OrderId         int64  `json:"orderId"`
	OrderListId     int64  `json:"orderListId"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	QuoteQty        string `json:"quoteQty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
	IsBuyer         bool   `json:"isBuyer"`
	IsMaker         bool   `json:"isMaker"`
	IsBestMatch     bool   `json:"isBestMatch"`
}

// ========== 钱包相关类型 ==========

// DepositAddressResponse 充值地址响应
type DepositAddressResponse struct {
	Address string `json:"address"`
	Coin    string `json:"coin"`
	Tag     string `json:"tag"`
	Url     string `json:"url"`
}

// DepositHistoryResponse 充值历史响应
type DepositHistoryResponse []DepositRecord

// DepositRecord 充值记录
type DepositRecord struct {
	Id            string `json:"id"`
	Amount        string `json:"amount"`
	Coin          string `json:"coin"`
	Network       string `json:"network"`
	Status        int    `json:"status"`
	Address       string `json:"address"`
	AddressTag    string `json:"addressTag"`
	TxId          string `json:"txId"`
	InsertTime    int64  `json:"insertTime"`
	TransferType  int    `json:"transferType"`
	UnlockConfirm string `json:"unlockConfirm"`
	ConfirmTimes  string `json:"confirmTimes"`
}

// WithdrawResponse 提现响应
type WithdrawResponse struct {
	Id string `json:"id"`
}

// WithdrawHistoryResponse 提现历史响应
type WithdrawHistoryResponse []WithdrawRecord

// WithdrawRecord 提现记录
type WithdrawRecord struct {
	Id              string `json:"id"`
	Amount          string `json:"amount"`
	TransactionFee  string `json:"transactionFee"`
	Coin            string `json:"coin"`
	Status          int    `json:"status"`
	Address         string `json:"address"`
	AddressTag      string `json:"addressTag"`
	TxId            string `json:"txId"`
	ApplyTime       string `json:"applyTime"`
	Network         string `json:"network"`
	TransferType    int    `json:"transferType"`
	WithdrawOrderId string `json:"withdrawOrderId"`
	Info            string `json:"info"`
	ConfirmNo       int    `json:"confirmNo"`
	WalletType      int    `json:"walletType"`
	TxKey           string `json:"txKey"`
	CompleteTime    string `json:"completeTime"`
}

// ========== 期货相关类型 ==========

// FuturesExchangeInfoResponse 期货交易所信息
type FuturesExchangeInfoResponse struct {
	ExchangeFilters []interface{}       `json:"exchangeFilters"`
	RateLimits      []RateLimit         `json:"rateLimits"`
	ServerTime      int64               `json:"serverTime"`
	Assets          []FuturesAsset      `json:"assets"`
	Symbols         []FuturesSymbolInfo `json:"symbols"`
	Timezone        string              `json:"timezone"`
}

// RateLimit 限流信息
type RateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
}

// FuturesAsset 期货资产
type FuturesAsset struct {
	Asset             string `json:"asset"`
	MarginAvailable   bool   `json:"marginAvailable"`
	AutoAssetExchange string `json:"autoAssetExchange"`
}

// FuturesSymbolInfo 期货交易对信息
type FuturesSymbolInfo struct {
	Symbol                string                   `json:"symbol"`
	Pair                  string                   `json:"pair"`
	ContractType          string                   `json:"contractType"`
	DeliveryDate          int64                    `json:"deliveryDate"`
	OnboardDate           int64                    `json:"onboardDate"`
	Status                string                   `json:"status"`
	MaintMarginPercent    string                   `json:"maintMarginPercent"`
	RequiredMarginPercent string                   `json:"requiredMarginPercent"`
	BaseAsset             string                   `json:"baseAsset"`
	QuoteAsset            string                   `json:"quoteAsset"`
	MarginAsset           string                   `json:"marginAsset"`
	PricePrecision        int                      `json:"pricePrecision"`
	QuantityPrecision     int                      `json:"quantityPrecision"`
	BaseAssetPrecision    int                      `json:"baseAssetPrecision"`
	QuotePrecision        int                      `json:"quotePrecision"`
	UnderlyingType        string                   `json:"underlyingType"`
	UnderlyingSubType     []string                 `json:"underlyingSubType"`
	SettlePlan            int                      `json:"settlePlan"`
	TriggerProtect        string                   `json:"triggerProtect"`
	LiquidationFee        string                   `json:"liquidationFee"`
	MarketTakeBound       string                   `json:"marketTakeBound"`
	Filters               []map[string]interface{} `json:"filters"`
	OrderTypes            []string                 `json:"orderTypes"`
	TimeInForce           []string                 `json:"timeInForce"`
}

// FuturesAccountResponse 期货账户响应
type FuturesAccountResponse struct {
	FeeTier                     int                   `json:"feeTier"`
	CanTrade                    bool                  `json:"canTrade"`
	CanDeposit                  bool                  `json:"canDeposit"`
	CanWithdraw                 bool                  `json:"canWithdraw"`
	UpdateTime                  int64                 `json:"updateTime"`
	MultiAssetsMargin           bool                  `json:"multiAssetsMargin"`
	TradeGroupId                int64                 `json:"tradeGroupId"`
	TotalInitialMargin          string                `json:"totalInitialMargin"`
	TotalMaintMargin            string                `json:"totalMaintMargin"`
	TotalWalletBalance          string                `json:"totalWalletBalance"`
	TotalUnrealizedProfit       string                `json:"totalUnrealizedProfit"`
	TotalMarginBalance          string                `json:"totalMarginBalance"`
	TotalPositionInitialMargin  string                `json:"totalPositionInitialMargin"`
	TotalOpenOrderInitialMargin string                `json:"totalOpenOrderInitialMargin"`
	TotalCrossWalletBalance     string                `json:"totalCrossWalletBalance"`
	TotalCrossUnPnl             string                `json:"totalCrossUnPnl"`
	AvailableBalance            string                `json:"availableBalance"`
	MaxWithdrawAmount           string                `json:"maxWithdrawAmount"`
	Assets                      []FuturesAccountAsset `json:"assets"`
	Positions                   []FuturesPositionRisk `json:"positions"`
}

// FuturesAccountAsset 期货账户资产
type FuturesAccountAsset struct {
	Asset                  string `json:"asset"`
	WalletBalance          string `json:"walletBalance"`
	UnrealizedProfit       string `json:"unrealizedProfit"`
	MarginBalance          string `json:"marginBalance"`
	MaintMargin            string `json:"maintMargin"`
	InitialMargin          string `json:"initialMargin"`
	PositionInitialMargin  string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	CrossWalletBalance     string `json:"crossWalletBalance"`
	CrossUnPnl             string `json:"crossUnPnl"`
	AvailableBalance       string `json:"availableBalance"`
	MaxWithdrawAmount      string `json:"maxWithdrawAmount"`
	MarginAvailable        bool   `json:"marginAvailable"`
	UpdateTime             int64  `json:"updateTime"`
}

// FuturesPositionRisk 期货持仓风险
type FuturesPositionRisk struct {
	Symbol           string `json:"symbol"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	UnRealizedProfit string `json:"unRealizedProfit"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	MaxNotionalValue string `json:"maxNotionalValue"`
	MarginType       string `json:"marginType"`
	IsolatedMargin   string `json:"isolatedMargin"`
	IsAutoAddMargin  string `json:"isAutoAddMargin"`
	PositionSide     string `json:"positionSide"`
	Notional         string `json:"notional"`
	IsolatedWallet   string `json:"isolatedWallet"`
	UpdateTime       int64  `json:"updateTime"`
	BidNotional      string `json:"bidNotional"`
	AskNotional      string `json:"askNotional"`
}

// ========== 保证金相关类型 ==========

// MarginAccountResponse 杠杆账户响应
type MarginAccountResponse struct {
	BorrowEnabled       bool                 `json:"borrowEnabled"`
	MarginLevel         string               `json:"marginLevel"`
	TotalAssetOfBtc     string               `json:"totalAssetOfBtc"`
	TotalLiabilityOfBtc string               `json:"totalLiabilityOfBtc"`
	TotalNetAssetOfBtc  string               `json:"totalNetAssetOfBtc"`
	TradeEnabled        bool                 `json:"tradeEnabled"`
	TransferEnabled     bool                 `json:"transferEnabled"`
	UserAssets          []MarginAccountAsset `json:"userAssets"`
}

// MarginAccountAsset 杠杆账户资产
type MarginAccountAsset struct {
	Asset    string `json:"asset"`
	Borrowed string `json:"borrowed"`
	Free     string `json:"free"`
	Interest string `json:"interest"`
	Locked   string `json:"locked"`
	NetAsset string `json:"netAsset"`
}

// FuturesBalanceResponse 期货余额响应
type FuturesBalanceResponse struct {
	AccountAlias       string `json:"accountAlias"`
	Asset              string `json:"asset"`
	Balance            string `json:"balance"`
	CrossWalletBalance string `json:"crossWalletBalance"`
	CrossUnPnl         string `json:"crossUnPnl"`
	AvailableBalance   string `json:"availableBalance"`
	MaxWithdrawAmount  string `json:"maxWithdrawAmount"`
	MarginAvailable    bool   `json:"marginAvailable"`
	UpdateTime         int64  `json:"updateTime"`
}

// ========== 错误响应类型 ==========

// ErrorResponse Binance错误响应
type ErrorResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// ========== WebSocket 消息类型 ==========

// WSMessage WebSocket消息基础结构
type WSMessage struct {
	Stream string      `json:"stream"`
	Data   interface{} `json:"data"`
}

// WSTickerMessage WebSocket ticker消息
type WSTickerMessage struct {
	EventType            string `json:"e"` // 事件类型
	EventTime            int64  `json:"E"` // 事件时间
	Symbol               string `json:"s"` // 交易对
	PriceChange          string `json:"p"` // 24小时价格变动
	PriceChangePercent   string `json:"P"` // 24小时价格变动百分比
	WeightedAveragePrice string `json:"w"` // 平均价格
	FirstTradePrice      string `json:"x"` // 前一笔成交价格
	LastPrice            string `json:"c"` // 最新成交价格
	LastQuantity         string `json:"Q"` // 最新成交量
	BestBidPrice         string `json:"b"` // 最优买单价
	BestBidQuantity      string `json:"B"` // 最优买单量
	BestAskPrice         string `json:"a"` // 最优卖单价
	BestAskQuantity      string `json:"A"` // 最优卖单量
	OpenPrice            string `json:"o"` // 开盘价
	HighPrice            string `json:"h"` // 最高价
	LowPrice             string `json:"l"` // 最低价
	Volume               string `json:"v"` // 成交量
	QuoteVolume          string `json:"q"` // 成交额
	OpenTime             int64  `json:"O"` // 统计开始时间
	CloseTime            int64  `json:"C"` // 统计结束时间
	FirstTradeId         int64  `json:"F"` // 首笔成交id
	LastTradeId          int64  `json:"L"` // 末笔成交id
	Count                int64  `json:"n"` // 成交笔数
}

// WSDepthMessage WebSocket深度消息
type WSDepthMessage struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateId int64      `json:"U"`
	FinalUpdateId int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

// WSTradeMessage WebSocket交易消息
type WSTradeMessage struct {
	EventType     string `json:"e"`
	EventTime     int64  `json:"E"`
	Symbol        string `json:"s"`
	TradeId       int64  `json:"t"`
	Price         string `json:"p"`
	Quantity      string `json:"q"`
	BuyerOrderId  int64  `json:"b"`
	SellerOrderId int64  `json:"a"`
	TradeTime     int64  `json:"T"`
	IsBuyerMaker  bool   `json:"m"`
	Placeholder   bool   `json:"M"` // 请忽略该字段
}

// WSKlineMessage WebSocket K线消息
type WSKlineMessage struct {
	EventType string      `json:"e"`
	EventTime int64       `json:"E"`
	Symbol    string      `json:"s"`
	Kline     WSKlineData `json:"k"`
}

// WSKlineData WebSocket K线数据
type WSKlineData struct {
	KlineStartTime           int64  `json:"t"`
	KlineCloseTime           int64  `json:"T"`
	Symbol                   string `json:"s"`
	Interval                 string `json:"i"`
	FirstTradeId             int64  `json:"f"`
	LastTradeId              int64  `json:"L"`
	OpenPrice                string `json:"o"`
	ClosePrice               string `json:"c"`
	HighPrice                string `json:"h"`
	LowPrice                 string `json:"l"`
	Volume                   string `json:"v"`
	NumberOfTrades           int    `json:"n"`
	IsClosed                 bool   `json:"x"`
	QuoteVolume              string `json:"q"`
	TakerBuyBaseAssetVolume  string `json:"V"`
	TakerBuyQuoteAssetVolume string `json:"Q"`
	Ignore                   string `json:"B"`
}

// ========== 辅助方法 ==========

// IsSuccess 检查响应是否成功
func (r *APIResponse) IsSuccess() bool {
	return r.Code == 0 || r.Code == 200
}

// GetErrorMessage 获取错误消息
func (r *APIResponse) GetErrorMessage() string {
	if r.Msg != "" {
		return r.Msg
	}
	return "unknown error"
}

// UnmarshalJSON 自定义JSON反序列化（用于处理不同的响应格式）
func (kr *KlineResponse) UnmarshalJSON(data []byte) error {
	var rawKlines [][]interface{}
	if err := json.Unmarshal(data, &rawKlines); err != nil {
		return err
	}
	*kr = KlineResponse(rawKlines)
	return nil
}

// ToKlineArray 转换为K线数组
func (kr *KlineResponse) ToKlineArray() [][]interface{} {
	return [][]interface{}(*kr)
}
