package bybit

// ========== 通用响应结构 ==========

// APIResponse Bybit API 通用响应
type APIResponse struct {
	RetCode    int         `json:"retCode"`
	RetMsg     string      `json:"retMsg"`
	Result     interface{} `json:"result"`
	RetExtInfo interface{} `json:"retExtInfo"`
	Time       int64       `json:"time"`
}

// ServerTimeResponse 服务器时间响应
type ServerTimeResponse struct {
	TimeSecond string `json:"timeSecond"`
	TimeNano   string `json:"timeNano"`
}

// ========== 市场数据相关类型 ==========

// InstrumentsResponse 交易对信息响应
type InstrumentsResponse struct {
	Category       string           `json:"category"`
	List           []InstrumentInfo `json:"list"`
	NextPageCursor string           `json:"nextPageCursor"`
}

// InstrumentInfo 交易对信息
type InstrumentInfo struct {
	Symbol          string `json:"symbol"`
	ContractType    string `json:"contractType"`
	Status          string `json:"status"`
	BaseCoin        string `json:"baseCoin"`
	QuoteCoin       string `json:"quoteCoin"`
	SettleCoin      string `json:"settleCoin"`
	LaunchTime      string `json:"launchTime"`
	DeliveryTime    string `json:"deliveryTime"`
	DeliveryFeeRate string `json:"deliveryFeeRate"`
	PriceScale      string `json:"priceScale"`
	LeverageFilter  struct {
		MinLeverage  string `json:"minLeverage"`
		MaxLeverage  string `json:"maxLeverage"`
		LeverageStep string `json:"leverageStep"`
	} `json:"leverageFilter"`
	PriceFilter struct {
		MinPrice string `json:"minPrice"`
		MaxPrice string `json:"maxPrice"`
		TickSize string `json:"tickSize"`
	} `json:"priceFilter"`
	LotSizeFilter struct {
		MaxOrderQty         string `json:"maxOrderQty"`
		MinOrderQty         string `json:"minOrderQty"`
		QtyStep             string `json:"qtyStep"`
		PostOnlyMaxOrderQty string `json:"postOnlyMaxOrderQty"`
	} `json:"lotSizeFilter"`
	UnifiedMarginTrade bool   `json:"unifiedMarginTrade"`
	FundingInterval    int    `json:"fundingInterval"`
	CopyTrading        string `json:"copyTrading"`
	UpperFundingRate   string `json:"upperFundingRate"`
	LowerFundingRate   string `json:"lowerFundingRate"`
}

// TickerResponse 24小时ticker响应
type TickerResponse struct {
	Category string       `json:"category"`
	List     []TickerData `json:"list"`
}

// TickerData ticker数据
type TickerData struct {
	Symbol                 string `json:"symbol"`
	Bid1Price              string `json:"bid1Price"`
	Bid1Size               string `json:"bid1Size"`
	Ask1Price              string `json:"ask1Price"`
	Ask1Size               string `json:"ask1Size"`
	LastPrice              string `json:"lastPrice"`
	PrevPrice24h           string `json:"prevPrice24h"`
	Price24hPcnt           string `json:"price24hPcnt"`
	HighPrice24h           string `json:"highPrice24h"`
	LowPrice24h            string `json:"lowPrice24h"`
	Turnover24h            string `json:"turnover24h"`
	Volume24h              string `json:"volume24h"`
	UsdIndexPrice          string `json:"usdIndexPrice"`
	MarkPrice              string `json:"markPrice"`
	IndexPrice             string `json:"indexPrice"`
	MarkPriceChangePcnt24h string `json:"markPriceChangePcnt24h"`
	NextFundingTime        string `json:"nextFundingTime"`
	FundingRate            string `json:"fundingRate"`
	OpenInterest           string `json:"openInterest"`
	OpenInterestValue      string `json:"openInterestValue"`
	DeliveryPrice          string `json:"deliveryPrice"`
	BasisRate              string `json:"basisRate"`
	DeliveryFeeRate        string `json:"deliveryFeeRate"`
	PreOpenPrice           string `json:"preOpenPrice"`
	PreQty                 string `json:"preQty"`
	CurPreListingPhase     string `json:"curPreListingPhase"`
}

// KlineResponse K线数据响应
type KlineResponse struct {
	Category string     `json:"category"`
	Symbol   string     `json:"symbol"`
	List     [][]string `json:"list"`
}

// TradesResponse 交易记录响应
type TradesResponse struct {
	Category string      `json:"category"`
	List     []TradeData `json:"list"`
}

// TradeData 交易数据
type TradeData struct {
	ExecId       string `json:"execId"`
	Symbol       string `json:"symbol"`
	Price        string `json:"price"`
	Size         string `json:"size"`
	Side         string `json:"side"`
	Time         string `json:"time"`
	IsBlockTrade bool   `json:"isBlockTrade"`
}

// OrderBookResponse 订单簿响应
type OrderBookResponse struct {
	Symbol    string     `json:"symbol"`
	Bids      [][]string `json:"b"`
	Asks      [][]string `json:"a"`
	Timestamp string     `json:"ts"`
	UpdateId  int64      `json:"u"`
}

// ========== 账户相关类型 ==========

// WalletBalanceResponse 钱包余额响应
type WalletBalanceResponse struct {
	List []WalletBalance `json:"list"`
}

// WalletBalance 钱包余额
type WalletBalance struct {
	TotalEquity            string        `json:"totalEquity"`
	AccountIMRate          string        `json:"accountIMRate"`
	TotalMarginBalance     string        `json:"totalMarginBalance"`
	TotalInitialMargin     string        `json:"totalInitialMargin"`
	AccountType            string        `json:"accountType"`
	TotalAvailableBalance  string        `json:"totalAvailableBalance"`
	TotalPerpUPL           string        `json:"totalPerpUPL"`
	TotalWalletBalance     string        `json:"totalWalletBalance"`
	AccountMMRate          string        `json:"accountMMRate"`
	TotalMaintenanceMargin string        `json:"totalMaintenanceMargin"`
	Coin                   []CoinBalance `json:"coin"`
}

// CoinBalance 货币余额
type CoinBalance struct {
	AvailableToBorrow   string `json:"availableToBorrow"`
	Bonus               string `json:"bonus"`
	AccruedInterest     string `json:"accruedInterest"`
	AvailableToWithdraw string `json:"availableToWithdraw"`
	TotalOrderIM        string `json:"totalOrderIM"`
	Equity              string `json:"equity"`
	TotalPositionMM     string `json:"totalPositionMM"`
	UsdValue            string `json:"usdValue"`
	UnrealisedPnl       string `json:"unrealisedPnl"`
	CollateralSwitch    bool   `json:"collateralSwitch"`
	BorrowAmount        string `json:"borrowAmount"`
	TotalPositionIM     string `json:"totalPositionIM"`
	WalletBalance       string `json:"walletBalance"`
	CumRealisedPnl      string `json:"cumRealisedPnl"`
	Locked              string `json:"locked"`
	MarginCollateral    bool   `json:"marginCollateral"`
	Coin                string `json:"coin"`
}

// ========== 交易相关类型 ==========

// OrderResponse 订单响应
type OrderResponse struct {
	OrderId            string `json:"orderId"`
	OrderLinkId        string `json:"orderLinkId"`
	Symbol             string `json:"symbol"`
	CreateTime         string `json:"createTime"`
	UpdateTime         string `json:"updateTime"`
	Side               string `json:"side"`
	OrderType          string `json:"orderType"`
	Qty                string `json:"qty"`
	Price              string `json:"price"`
	TimeInForce        string `json:"timeInForce"`
	OrderStatus        string `json:"orderStatus"`
	LastPriceOnCreated string `json:"lastPriceOnCreated"`
	ReduceOnly         bool   `json:"reduceOnly"`
	LeavesQty          string `json:"leavesQty"`
	LeavesValue        string `json:"leavesValue"`
	CumExecQty         string `json:"cumExecQty"`
	CumExecValue       string `json:"cumExecValue"`
	AvgPrice           string `json:"avgPrice"`
	StopOrderType      string `json:"stopOrderType"`
	TriggerPrice       string `json:"triggerPrice"`
	TakeProfit         string `json:"takeProfit"`
	StopLoss           string `json:"stopLoss"`
	TpTriggerBy        string `json:"tpTriggerBy"`
	SlTriggerBy        string `json:"slTriggerBy"`
	TriggerDirection   int    `json:"triggerDirection"`
	TriggerBy          string `json:"triggerBy"`
	CloseOnTrigger     bool   `json:"closeOnTrigger"`
	Category           string `json:"category"`
	PlaceType          string `json:"placeType"`
	SmpType            string `json:"smpType"`
	SmpGroup           int    `json:"smpGroup"`
	SmpOrderId         string `json:"smpOrderId"`
	RejectReason       string `json:"rejectReason"`
}

// OrderListResponse 订单列表响应
type OrderListResponse struct {
	Category       string          `json:"category"`
	List           []OrderResponse `json:"list"`
	NextPageCursor string          `json:"nextPageCursor"`
}

// ExecutionResponse 成交记录响应
type ExecutionResponse struct {
	Category       string          `json:"category"`
	List           []ExecutionData `json:"list"`
	NextPageCursor string          `json:"nextPageCursor"`
}

// ExecutionData 成交数据
type ExecutionData struct {
	Symbol          string `json:"symbol"`
	OrderId         string `json:"orderId"`
	OrderLinkId     string `json:"orderLinkId"`
	Side            string `json:"side"`
	OrderQty        string `json:"orderQty"`
	OrderPrice      string `json:"orderPrice"`
	OrderType       string `json:"orderType"`
	ExecFee         string `json:"execFee"`
	ExecId          string `json:"execId"`
	ExecPrice       string `json:"execPrice"`
	ExecQty         string `json:"execQty"`
	ExecTime        string `json:"execTime"`
	ExecType        string `json:"execType"`
	ExecValue       string `json:"execValue"`
	FeeRate         string `json:"feeRate"`
	TradeIv         string `json:"tradeIv"`
	MarkIv          string `json:"markIv"`
	BlockTradeId    string `json:"blockTradeId"`
	MarkPrice       string `json:"markPrice"`
	IndexPrice      string `json:"indexPrice"`
	UnderlyingPrice string `json:"underlyingPrice"`
	ClosedSize      string `json:"closedSize"`
	NextPageCursor  string `json:"nextPageCursor"`
	Category        string `json:"category"`
	FeeCurrency     string `json:"feeCurrency"`
	IsMaker         bool   `json:"isMaker"`
}

// ========== 持仓相关类型 ==========

// PositionResponse 持仓响应
type PositionResponse struct {
	Category       string         `json:"category"`
	List           []PositionData `json:"list"`
	NextPageCursor string         `json:"nextPageCursor"`
}

// PositionData 持仓数据
type PositionData struct {
	Symbol                 string `json:"symbol"`
	Leverage               string `json:"leverage"`
	AvgPrice               string `json:"avgPrice"`
	LiqPrice               string `json:"liqPrice"`
	RiskLimitValue         string `json:"riskLimitValue"`
	TakeProfit             string `json:"takeProfit"`
	PositionValue          string `json:"positionValue"`
	TpslMode               string `json:"tpslMode"`
	RiskId                 int    `json:"riskId"`
	TrailingStop           string `json:"trailingStop"`
	UnrealisedPnl          string `json:"unrealisedPnl"`
	MarkPrice              string `json:"markPrice"`
	CumRealisedPnl         string `json:"cumRealisedPnl"`
	PositionMM             string `json:"positionMM"`
	CreatedTime            string `json:"createdTime"`
	PositionIdx            int    `json:"positionIdx"`
	PositionIM             string `json:"positionIM"`
	UpdatedTime            string `json:"updatedTime"`
	Side                   string `json:"side"`
	BustPrice              string `json:"bustPrice"`
	Size                   string `json:"size"`
	PositionStatus         string `json:"positionStatus"`
	StopLoss               string `json:"stopLoss"`
	TradeMode              int    `json:"tradeMode"`
	AutoAddMargin          int    `json:"autoAddMargin"`
	AdlRankIndicator       int    `json:"adlRankIndicator"`
	LeverageSysUpdatedTime string `json:"leverageSysUpdatedTime"`
	MmrSysUpdatedTime      string `json:"mmrSysUpdatedTime"`
	Seq                    int64  `json:"seq"`
	IsReduceOnly           bool   `json:"isReduceOnly"`
}

// ========== 资金相关类型 ==========

// FundingRateResponse 资金费率响应
type FundingRateResponse struct {
	Category string            `json:"category"`
	List     []FundingRateData `json:"list"`
}

// FundingRateData 资金费率数据
type FundingRateData struct {
	Symbol               string `json:"symbol"`
	FundingRate          string `json:"fundingRate"`
	FundingRateTimestamp string `json:"fundingRateTimestamp"`
}

// ========== WebSocket 消息类型 ==========

// WSMessage WebSocket消息基础结构
type WSMessage struct {
	Topic string      `json:"topic"`
	Type  string      `json:"type"`
	Data  interface{} `json:"data"`
	Ts    int64       `json:"ts"`
}

// WSTickerMessage WebSocket ticker消息
type WSTickerMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Symbol                 string `json:"symbol"`
		TickDirection          string `json:"tickDirection"`
		Price24hPcnt           string `json:"price24hPcnt"`
		LastPrice              string `json:"lastPrice"`
		PrevPrice24h           string `json:"prevPrice24h"`
		HighPrice24h           string `json:"highPrice24h"`
		LowPrice24h            string `json:"lowPrice24h"`
		Turnover24h            string `json:"turnover24h"`
		Volume24h              string `json:"volume24h"`
		MarkPrice              string `json:"markPrice"`
		IndexPrice             string `json:"indexPrice"`
		OpenInterest           string `json:"openInterest"`
		OpenInterestValue      string `json:"openInterestValue"`
		FundingRate            string `json:"fundingRate"`
		NextFundingTime        string `json:"nextFundingTime"`
		PredictedDeliveryPrice string `json:"predictedDeliveryPrice"`
		BasisRate              string `json:"basisRate"`
		DeliveryFeeRate        string `json:"deliveryFeeRate"`
		DeliveryTime           string `json:"deliveryTime"`
		Ask1Size               string `json:"ask1Size"`
		Bid1Price              string `json:"bid1Price"`
		Ask1Price              string `json:"ask1Price"`
		Bid1Size               string `json:"bid1Size"`
	} `json:"data"`
	Ts int64 `json:"ts"`
}

// WSOrderBookMessage WebSocket订单簿消息
type WSOrderBookMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Symbol   string     `json:"s"`
		Bids     [][]string `json:"b"`
		Asks     [][]string `json:"a"`
		UpdateId int64      `json:"u"`
		Seq      int64      `json:"seq"`
	} `json:"data"`
	Ts int64 `json:"ts"`
}

// WSTradeMessage WebSocket交易消息
type WSTradeMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  []struct {
		Timestamp    int64  `json:"T"`
		Symbol       string `json:"s"`
		Side         string `json:"S"`
		Size         string `json:"v"`
		Price        string `json:"p"`
		Direction    string `json:"L"`
		TradeId      string `json:"i"`
		IsBlockTrade bool   `json:"BT"`
	} `json:"data"`
	Ts int64 `json:"ts"`
}

// WSKlineMessage WebSocket K线消息
type WSKlineMessage struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  []struct {
		Start     int64  `json:"start"`
		End       int64  `json:"end"`
		Interval  string `json:"interval"`
		Open      string `json:"open"`
		Close     string `json:"close"`
		High      string `json:"high"`
		Low       string `json:"low"`
		Volume    string `json:"volume"`
		Turnover  string `json:"turnover"`
		Confirm   bool   `json:"confirm"`
		Timestamp int64  `json:"timestamp"`
	} `json:"data"`
	Ts int64 `json:"ts"`
}

// ========== 错误响应类型 ==========

// ErrorResponse Bybit错误响应
type ErrorResponse struct {
	RetCode    int    `json:"retCode"`
	RetMsg     string `json:"retMsg"`
	RetExtInfo struct {
		List []struct {
			Code string `json:"code"`
			Msg  string `json:"msg"`
		} `json:"list"`
	} `json:"retExtInfo"`
	Time int64 `json:"time"`
}

// ========== 辅助方法 ==========

// IsSuccess 检查响应是否成功
func (r *APIResponse) IsSuccess() bool {
	return r.RetCode == 0
}

// GetErrorMessage 获取错误消息
func (r *APIResponse) GetErrorMessage() string {
	if r.RetMsg != "" {
		return r.RetMsg
	}
	return "unknown error"
}

// IsSuccess 检查错误响应
func (r *ErrorResponse) IsSuccess() bool {
	return r.RetCode == 0
}

// GetErrorMessage 获取错误消息
func (r *ErrorResponse) GetErrorMessage() string {
	if r.RetMsg != "" {
		return r.RetMsg
	}
	return "unknown error"
}
