package binance

import (
	"encoding/json"
	"strconv"
	"strings"

	"datahive/pkg/ccxt"
	"datahive/pkg/utils"
)

// ========== 市场数据解析器 ==========

// parseSpotMarket 解析现货市场信息
func (b *Binance) parseSpotMarket(symbol *SymbolInfo) *ccxt.Market {
	base := symbol.BaseAsset
	quote := symbol.QuoteAsset

	// 构建标准化符号
	standardSymbol := base + "/" + quote

	// 解析过滤器
	precision := ccxt.MarketPrecision{
		Amount: float64(symbol.BaseAssetPrecision),
		Price:  float64(symbol.QuotePrecision),
		Cost:   float64(symbol.QuotePrecision),
	}

	limits := b.parseMarketLimits(symbol.Filters)

	return &ccxt.Market{
		ID:           symbol.Symbol,
		Symbol:       standardSymbol,
		Base:         base,
		Quote:        quote,
		Type:         ccxt.MarketTypeSpot,
		Spot:         true,
		Margin:       symbol.IsMarginTradingAllowed,
		Swap:         false,
		Future:       false,
		Option:       false,
		Active:       symbol.Status == "TRADING",
		Contract:     false,
		Linear:       false,
		Inverse:      false,
		Taker:        0.001, // 默认费率，可从账户信息获取具体费率
		Maker:        0.001,
		ContractSize: 1.0,
		Precision:    precision,
		Limits:       limits,
		Info:         b.symbolInfoToMap(symbol),
	}
}

// parseFuturesMarket 解析期货市场信息
func (b *Binance) parseFuturesMarket(symbol *FuturesSymbolInfo) *ccxt.Market {
	base := symbol.BaseAsset
	quote := symbol.QuoteAsset
	settle := symbol.MarginAsset

	// 构建标准化符号 - 期货市场总是包含settle信息以区分现货
	var standardSymbol string
	if settle != "" {
		// 期货市场总是显示结算资产，避免与现货市场冲突
		standardSymbol = base + "/" + quote + ":" + settle
	} else {
		// 如果没有settle信息，添加默认后缀区分期货
		standardSymbol = base + "/" + quote + ":FUTURES"
	}

	precision := ccxt.MarketPrecision{
		Amount: float64(symbol.QuantityPrecision),
		Price:  float64(symbol.PricePrecision),
		Cost:   float64(symbol.PricePrecision),
	}

	limits := b.parseFuturesMarketLimits(symbol.Filters)

	// 确定合约类型
	isSwap := symbol.ContractType == "PERPETUAL"
	isFuture := symbol.ContractType != "PERPETUAL"

	return &ccxt.Market{
		ID:           symbol.Symbol,
		Symbol:       standardSymbol,
		Base:         base,
		Quote:        quote,
		Settle:       settle,
		Type:         ccxt.MarketTypeSwap,
		Spot:         false,
		Margin:       true,
		Swap:         isSwap,
		Future:       isFuture,
		Option:       false,
		Active:       symbol.Status == "TRADING",
		Contract:     true,
		Linear:       symbol.MarginAsset == symbol.QuoteAsset,
		Inverse:      symbol.MarginAsset == symbol.BaseAsset,
		Taker:        0.0004, // 期货默认费率
		Maker:        0.0002,
		ContractSize: 1.0,
		Expiry:       symbol.DeliveryDate,
		Precision:    precision,
		Limits:       limits,
		Info:         b.futuresSymbolInfoToMap(symbol),
	}
}

// parseMarketLimits 解析现货市场限制
func (b *Binance) parseMarketLimits(filters []map[string]interface{}) ccxt.MarketLimits {
	limits := ccxt.MarketLimits{
		Leverage: ccxt.LimitRange{Min: 1, Max: 1}, // 现货无杠杆
		Amount:   ccxt.LimitRange{},
		Price:    ccxt.LimitRange{},
		Cost:     ccxt.LimitRange{},
	}

	for _, filter := range filters {
		filterType := b.SafeString(filter, "filterType", "")

		switch filterType {
		case "LOT_SIZE":
			limits.Amount.Min = b.SafeFloat(filter, "minQty", 0)
			limits.Amount.Max = b.SafeFloat(filter, "maxQty", 0)

		case "PRICE_FILTER":
			limits.Price.Min = b.SafeFloat(filter, "minPrice", 0)
			limits.Price.Max = b.SafeFloat(filter, "maxPrice", 0)

		case "MIN_NOTIONAL":
			limits.Cost.Min = b.SafeFloat(filter, "minNotional", 0)
		}
	}

	return limits
}

// parseFuturesMarketLimits 解析期货市场限制
func (b *Binance) parseFuturesMarketLimits(filters []map[string]interface{}) ccxt.MarketLimits {
	limits := ccxt.MarketLimits{
		Leverage: ccxt.LimitRange{Min: 1, Max: 125}, // 期货默认杠杆范围
		Amount:   ccxt.LimitRange{},
		Price:    ccxt.LimitRange{},
		Cost:     ccxt.LimitRange{},
	}

	for _, filter := range filters {
		filterType := b.SafeString(filter, "filterType", "")

		switch filterType {
		case "LOT_SIZE":
			limits.Amount.Min = b.SafeFloat(filter, "minQty", 0)
			limits.Amount.Max = b.SafeFloat(filter, "maxQty", 0)

		case "PRICE_FILTER":
			limits.Price.Min = b.SafeFloat(filter, "minPrice", 0)
			limits.Price.Max = b.SafeFloat(filter, "maxPrice", 0)

		case "MIN_NOTIONAL":
			limits.Cost.Min = b.SafeFloat(filter, "minNotional", 0)
		}
	}

	return limits
}

// parseTicker 解析ticker数据
func (b *Binance) parseTicker(ticker *Ticker24HrResponse, market *ccxt.Market) *ccxt.Ticker {
	timestamp := ticker.CloseTime

	// 直接解析字符串价格数据
	high, _ := strconv.ParseFloat(ticker.HighPrice, 64)
	low, _ := strconv.ParseFloat(ticker.LowPrice, 64)
	bid, _ := strconv.ParseFloat(ticker.BidPrice, 64)
	bidVolume, _ := strconv.ParseFloat(ticker.BidQty, 64)
	ask, _ := strconv.ParseFloat(ticker.AskPrice, 64)
	askVolume, _ := strconv.ParseFloat(ticker.AskQty, 64)
	vwap, _ := strconv.ParseFloat(ticker.WeightedAvgPrice, 64)
	open, _ := strconv.ParseFloat(ticker.OpenPrice, 64)
	close, _ := strconv.ParseFloat(ticker.LastPrice, 64)
	last, _ := strconv.ParseFloat(ticker.LastPrice, 64)
	previousClose, _ := strconv.ParseFloat(ticker.PrevClosePrice, 64)
	change, _ := strconv.ParseFloat(ticker.PriceChange, 64)
	percentage, _ := strconv.ParseFloat(ticker.PriceChangePercent, 64)
	baseVolume, _ := strconv.ParseFloat(ticker.Volume, 64)
	quoteVolume, _ := strconv.ParseFloat(ticker.QuoteVolume, 64)

	return &ccxt.Ticker{
		Symbol:        utils.SanitizeUTF8(market.Symbol),
		TimeStamp:     timestamp,
		Datetime:      utils.SanitizeUTF8(b.ISO8601(timestamp)),
		High:          high,
		Low:           low,
		Bid:           bid,
		BidVolume:     bidVolume,
		Ask:           ask,
		AskVolume:     askVolume,
		Vwap:          vwap,
		Open:          open,
		Close:         close,
		Last:          last,
		PreviousClose: previousClose,
		Change:        change,
		Percentage:    percentage,
		Average:       (bid + ask) / 2, // 计算平均价格
		BaseVolume:    baseVolume,
		QuoteVolume:   quoteVolume,
		Info:          b.tickerToMap(ticker),
	}
}

// parseOrderBook 解析订单簿数据
func (b *Binance) parseOrderBook(depth *DepthResponse, symbol string) *ccxt.OrderBook {
	timestamp := b.Milliseconds()

	// 解析买单
	bidPrices := make([]float64, 0, len(depth.Bids))
	bidSizes := make([]float64, 0, len(depth.Bids))
	for _, bid := range depth.Bids {
		if len(bid) >= 2 {
			price, _ := strconv.ParseFloat(bid[0], 64)
			amount, _ := strconv.ParseFloat(bid[1], 64)
			bidPrices = append(bidPrices, price)
			bidSizes = append(bidSizes, amount)
		}
	}

	// 解析卖单
	askPrices := make([]float64, 0, len(depth.Asks))
	askSizes := make([]float64, 0, len(depth.Asks))
	for _, ask := range depth.Asks {
		if len(ask) >= 2 {
			price, _ := strconv.ParseFloat(ask[0], 64)
			amount, _ := strconv.ParseFloat(ask[1], 64)
			askPrices = append(askPrices, price)
			askSizes = append(askSizes, amount)
		}
	}

	return &ccxt.OrderBook{
		Symbol:    symbol,
		Bids:      ccxt.OrderBookSide{Price: bidPrices, Size: bidSizes},
		Asks:      ccxt.OrderBookSide{Price: askPrices, Size: askSizes},
		TimeStamp: timestamp,
		Datetime:  b.ISO8601(timestamp),
		Nonce:     depth.LastUpdateId,
		Info:      b.depthToMap(depth),
	}
}

// parseTrade 解析交易数据
func (b *Binance) parseTrade(trade *TradeResponse, symbol string) *ccxt.Trade {
	timestamp := trade.Time
	price := b.SafeFloat(map[string]interface{}{"value": trade.Price}, "value", 0)
	amount := b.SafeFloat(map[string]interface{}{"value": trade.Qty}, "value", 0)

	var side string
	if trade.IsBuyerMaker {
		side = "sell"
	} else {
		side = "buy"
	}

	return &ccxt.Trade{
		ID:           strconv.FormatInt(trade.ID, 10),
		Symbol:       symbol,
		Timestamp:    timestamp,
		Datetime:     b.ISO8601(timestamp),
		Side:         side,
		Amount:       amount,
		Price:        price,
		Cost:         price * amount,
		TakerOrMaker: "",
		Info:         b.tradeToMap(trade),
	}
}

// parseKline 解析K线数据
func (b *Binance) parseKline(kline []interface{}) *ccxt.OHLCV {
	if len(kline) < 6 {
		return nil
	}

	timestamp := int64(kline[0].(float64))
	open := b.SafeFloat(map[string]interface{}{"value": kline[1]}, "value", 0)
	high := b.SafeFloat(map[string]interface{}{"value": kline[2]}, "value", 0)
	low := b.SafeFloat(map[string]interface{}{"value": kline[3]}, "value", 0)
	close := b.SafeFloat(map[string]interface{}{"value": kline[4]}, "value", 0)
	volume := b.SafeFloat(map[string]interface{}{"value": kline[5]}, "value", 0)

	return &ccxt.OHLCV{
		Timestamp: timestamp,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
	}
}

// ========== 账户数据解析器 ==========

// parseSpotBalance 解析现货余额
func (b *Binance) parseSpotBalance(account *AccountResponse) *ccxt.Account {
	balances := make(map[string]ccxt.Balance)
	free := make(map[string]float64)
	used := make(map[string]float64)
	total := make(map[string]float64)

	for _, balance := range account.Balances {
		asset := balance.Asset
		freeAmount := b.SafeFloat(map[string]interface{}{"value": balance.Free}, "value", 0)
		lockedAmount := b.SafeFloat(map[string]interface{}{"value": balance.Locked}, "value", 0)
		totalAmount := freeAmount + lockedAmount

		if totalAmount > 0 {
			balances[asset] = ccxt.Balance{
				Free:  freeAmount,
				Used:  lockedAmount,
				Total: totalAmount,
			}
			free[asset] = freeAmount
			used[asset] = lockedAmount
			total[asset] = totalAmount
		}
	}

	return &ccxt.Account{
		Info:      b.accountToMap(account),
		Type:      "spot",
		Balances:  balances,
		Free:      free,
		Used:      used,
		Total:     total,
		Timestamp: account.UpdateTime,
		Datetime:  b.ISO8601(account.UpdateTime),
	}
}

// ========== 交易数据解析器 ==========

// parseOrder 解析订单数据
func (b *Binance) parseOrder(order *OrderResponse, market *ccxt.Market) *ccxt.Order {
	var symbol string
	if market != nil {
		symbol = market.Symbol
	} else {
		symbol = order.Symbol
	}

	timestamp := order.Time
	lastTradeTimestamp := order.UpdateTime

	// 解析订单状态
	status := b.parseOrderStatus(order.Status)

	// 解析订单类型和方向
	orderType := strings.ToLower(order.Type)
	side := strings.ToLower(order.Side)

	// 解析价格和数量
	price := b.SafeFloat(map[string]interface{}{"value": order.Price}, "value", 0)
	amount := b.SafeFloat(map[string]interface{}{"value": order.OrigQty}, "value", 0)
	filled := b.SafeFloat(map[string]interface{}{"value": order.ExecutedQty}, "value", 0)
	remaining := amount - filled
	cost := b.SafeFloat(map[string]interface{}{"value": order.CummulativeQuoteQty}, "value", 0)

	// 计算平均价格
	var average float64
	if filled > 0 && cost > 0 {
		average = cost / filled
	}

	// 解析手续费
	fee := ccxt.Fee{}
	if len(order.Fills) > 0 {
		var totalFee float64
		var feeCurrency string
		for _, fill := range order.Fills {
			fillFee := b.SafeFloat(map[string]interface{}{"value": fill.Commission}, "value", 0)
			totalFee += fillFee
			if feeCurrency == "" {
				feeCurrency = fill.CommissionAsset
			}
		}
		fee = ccxt.Fee{
			Currency: feeCurrency,
			Cost:     totalFee,
		}
	}

	// 解析成交记录
	trades := make([]ccxt.Trade, len(order.Fills))
	for i, fill := range order.Fills {
		fillPrice := b.SafeFloat(map[string]interface{}{"value": fill.Price}, "value", 0)
		fillAmount := b.SafeFloat(map[string]interface{}{"value": fill.Qty}, "value", 0)
		fillCost := fillPrice * fillAmount

		trades[i] = ccxt.Trade{
			ID:     strconv.FormatInt(fill.TradeId, 10),
			Symbol: symbol,
			Side:   side,
			Amount: fillAmount,
			Price:  fillPrice,
			Cost:   fillCost,
			Fee: ccxt.Fee{
				Currency: fill.CommissionAsset,
				Cost:     b.SafeFloat(map[string]interface{}{"value": fill.Commission}, "value", 0),
			},
		}
	}

	return &ccxt.Order{
		ID:                 strconv.FormatInt(order.OrderId, 10),
		ClientOrderId:      order.ClientOrderId,
		Timestamp:          timestamp,
		Datetime:           b.ISO8601(timestamp),
		LastTradeTimestamp: lastTradeTimestamp,
		Symbol:             symbol,
		Type:               orderType,
		TimeInForce:        order.TimeInForce,
		Side:               side,
		Amount:             amount,
		Price:              price,
		Average:            average,
		Filled:             filled,
		Remaining:          remaining,
		Cost:               cost,
		Status:             status,
		Fee:                fee,
		Trades:             trades,
		Info:               b.orderToMap(order),
	}
}

// parseOrderStatus 解析订单状态
func (b *Binance) parseOrderStatus(status string) string {
	statusMap := map[string]string{
		"NEW":              ccxt.OrderStatusOpen,
		"PARTIALLY_FILLED": ccxt.OrderStatusPartiallyFilled,
		"FILLED":           ccxt.OrderStatusFilled,
		"CANCELED":         ccxt.OrderStatusCanceled,
		"PENDING_CANCEL":   ccxt.OrderStatusCanceled,
		"REJECTED":         ccxt.OrderStatusRejected,
		"EXPIRED":          ccxt.OrderStatusExpired,
	}

	if mappedStatus, exists := statusMap[status]; exists {
		return mappedStatus
	}

	return status
}

// parseMyTrade 解析我的交易数据
func (b *Binance) parseMyTrade(trade *MyTradeResponse, symbol string) *ccxt.Trade {
	timestamp := trade.Time
	price := b.SafeFloat(map[string]interface{}{"value": trade.Price}, "value", 0)
	amount := b.SafeFloat(map[string]interface{}{"value": trade.Qty}, "value", 0)
	cost := b.SafeFloat(map[string]interface{}{"value": trade.QuoteQty}, "value", 0)

	var side string
	if trade.IsBuyer {
		side = "buy"
	} else {
		side = "sell"
	}

	var takerOrMaker string
	if trade.IsMaker {
		takerOrMaker = "maker"
	} else {
		takerOrMaker = "taker"
	}

	fee := ccxt.Fee{
		Currency: trade.CommissionAsset,
		Cost:     b.SafeFloat(map[string]interface{}{"value": trade.Commission}, "value", 0),
	}

	return &ccxt.Trade{
		ID:           strconv.FormatInt(trade.Id, 10),
		Order:        strconv.FormatInt(trade.OrderId, 10),
		Symbol:       symbol,
		Timestamp:    timestamp,
		Datetime:     b.ISO8601(timestamp),
		Side:         side,
		Amount:       amount,
		Price:        price,
		Cost:         cost,
		TakerOrMaker: takerOrMaker,
		Fee:          fee,
		Info:         b.myTradeToMap(trade),
	}
}

// parseDeposit 解析充值数据
func (b *Binance) parseDeposit(deposit *DepositRecord) *ccxt.Transaction {
	timestamp := deposit.InsertTime

	// 解析状态
	var status string
	switch deposit.Status {
	case 0:
		status = "pending"
	case 6:
		status = "credited"
	case 1:
		status = "success"
	default:
		status = "unknown"
	}

	amount := b.SafeFloat(map[string]interface{}{"value": deposit.Amount}, "value", 0)

	return &ccxt.Transaction{
		ID:        deposit.Id,
		TxID:      deposit.TxId,
		Currency:  deposit.Coin,
		Amount:    amount,
		Address:   deposit.Address,
		Tag:       deposit.AddressTag,
		Status:    status,
		Type:      "deposit",
		Network:   deposit.Network,
		Fee:       ccxt.Fee{},
		Timestamp: timestamp,
		Datetime:  b.ISO8601(timestamp),
		Info:      b.depositRecordToMap(deposit),
	}
}

// parseWithdraw 解析提现数据
func (b *Binance) parseWithdraw(withdraw *WithdrawRecord) *ccxt.Transaction {
	timestamp := b.SafeInt(map[string]interface{}{"value": withdraw.ApplyTime}, "value", 0)

	// 解析状态
	var status string
	switch withdraw.Status {
	case 0:
		status = "email sent"
	case 1:
		status = "cancelled"
	case 2:
		status = "awaiting approval"
	case 3:
		status = "rejected"
	case 4:
		status = "processing"
	case 5:
		status = "failure"
	case 6:
		status = "completed"
	default:
		status = "unknown"
	}

	amount := b.SafeFloat(map[string]interface{}{"value": withdraw.Amount}, "value", 0)
	fee := b.SafeFloat(map[string]interface{}{"value": withdraw.TransactionFee}, "value", 0)

	return &ccxt.Transaction{
		ID:        withdraw.Id,
		TxID:      withdraw.TxId,
		Currency:  withdraw.Coin,
		Amount:    amount,
		Address:   withdraw.Address,
		Tag:       withdraw.AddressTag,
		Status:    status,
		Type:      "withdrawal",
		Network:   withdraw.Network,
		Fee:       ccxt.Fee{Currency: withdraw.Coin, Cost: fee},
		Timestamp: timestamp,
		Datetime:  b.ISO8601(timestamp),
		Info:      b.withdrawRecordToMap(withdraw),
	}
}

// parsePosition 解析持仓数据
func (b *Binance) parsePosition(position *FuturesPositionRisk) *ccxt.Position {
	symbol := position.Symbol

	// 如果持仓量为0，跳过
	positionAmt := b.SafeFloat(map[string]interface{}{"value": position.PositionAmt}, "value", 0)
	if positionAmt == 0 {
		return nil
	}

	// 确定持仓方向
	var side string
	if positionAmt > 0 {
		side = "long"
	} else {
		side = "short"
		positionAmt = -positionAmt // 转为正数
	}

	entryPrice := b.SafeFloat(map[string]interface{}{"value": position.EntryPrice}, "value", 0)
	markPrice := b.SafeFloat(map[string]interface{}{"value": position.MarkPrice}, "value", 0)
	unrealizedPnl := b.SafeFloat(map[string]interface{}{"value": position.UnRealizedProfit}, "value", 0)
	leverage := b.SafeFloat(map[string]interface{}{"value": position.Leverage}, "value", 0)
	notionalValue := b.SafeFloat(map[string]interface{}{"value": position.Notional}, "value", 0)
	liquidationPrice := b.SafeFloat(map[string]interface{}{"value": position.LiquidationPrice}, "value", 0)

	return &ccxt.Position{
		Symbol:           symbol,
		Side:             side,
		Size:             positionAmt,
		EntryPrice:       entryPrice,
		MarkPrice:        markPrice,
		UnrealizedPnl:    unrealizedPnl,
		Leverage:         leverage,
		NotionalValue:    notionalValue,
		LiquidationPrice: liquidationPrice,
		Timestamp:        position.UpdateTime,
		Datetime:         b.ISO8601(position.UpdateTime),
		Info:             b.positionToMap(position),
	}
}

// ========== 辅助转换方法 ==========

// symbolInfoToMap 将SymbolInfo转换为map
func (b *Binance) symbolInfoToMap(symbol *SymbolInfo) map[string]interface{} {
	return map[string]interface{}{
		"symbol":                 symbol.Symbol,
		"status":                 symbol.Status,
		"baseAsset":              symbol.BaseAsset,
		"baseAssetPrecision":     symbol.BaseAssetPrecision,
		"quoteAsset":             symbol.QuoteAsset,
		"quotePrecision":         symbol.QuotePrecision,
		"quoteAssetPrecision":    symbol.QuoteAssetPrecision,
		"orderTypes":             symbol.OrderTypes,
		"icebergAllowed":         symbol.IcebergAllowed,
		"ocoAllowed":             symbol.OcoAllowed,
		"isSpotTradingAllowed":   symbol.IsSpotTradingAllowed,
		"isMarginTradingAllowed": symbol.IsMarginTradingAllowed,
		"filters":                symbol.Filters,
		"permissions":            symbol.Permissions,
	}
}

// futuresSymbolInfoToMap 将FuturesSymbolInfo转换为map
func (b *Binance) futuresSymbolInfoToMap(symbol *FuturesSymbolInfo) map[string]interface{} {
	return map[string]interface{}{
		"symbol":            symbol.Symbol,
		"pair":              symbol.Pair,
		"contractType":      symbol.ContractType,
		"deliveryDate":      symbol.DeliveryDate,
		"onboardDate":       symbol.OnboardDate,
		"status":            symbol.Status,
		"baseAsset":         symbol.BaseAsset,
		"quoteAsset":        symbol.QuoteAsset,
		"marginAsset":       symbol.MarginAsset,
		"pricePrecision":    symbol.PricePrecision,
		"quantityPrecision": symbol.QuantityPrecision,
		"underlyingType":    symbol.UnderlyingType,
		"underlyingSubType": symbol.UnderlyingSubType,
		"settlePlan":        symbol.SettlePlan,
		"triggerProtect":    symbol.TriggerProtect,
		"filters":           symbol.Filters,
		"orderTypes":        symbol.OrderTypes,
		"timeInForce":       symbol.TimeInForce,
	}
}

// tickerToMap 将Ticker转换为map
func (b *Binance) tickerToMap(ticker *Ticker24HrResponse) map[string]interface{} {
	return map[string]interface{}{
		"symbol":             ticker.Symbol,
		"priceChange":        ticker.PriceChange,
		"priceChangePercent": ticker.PriceChangePercent,
		"weightedAvgPrice":   ticker.WeightedAvgPrice,
		"prevClosePrice":     ticker.PrevClosePrice,
		"lastPrice":          ticker.LastPrice,
		"lastQty":            ticker.LastQty,
		"bidPrice":           ticker.BidPrice,
		"bidQty":             ticker.BidQty,
		"askPrice":           ticker.AskPrice,
		"askQty":             ticker.AskQty,
		"openPrice":          ticker.OpenPrice,
		"highPrice":          ticker.HighPrice,
		"lowPrice":           ticker.LowPrice,
		"volume":             ticker.Volume,
		"quoteVolume":        ticker.QuoteVolume,
		"openTime":           ticker.OpenTime,
		"closeTime":          ticker.CloseTime,
		"count":              ticker.Count,
	}
}

// depthToMap 将DepthResponse转换为map
func (b *Binance) depthToMap(depth *DepthResponse) map[string]interface{} {
	return map[string]interface{}{
		"lastUpdateId": depth.LastUpdateId,
		"bids":         depth.Bids,
		"asks":         depth.Asks,
	}
}

// tradeToMap 将TradeResponse转换为map
func (b *Binance) tradeToMap(trade *TradeResponse) map[string]interface{} {
	return map[string]interface{}{
		"id":           trade.ID,
		"price":        trade.Price,
		"qty":          trade.Qty,
		"quoteQty":     trade.QuoteQty,
		"time":         trade.Time,
		"isBuyerMaker": trade.IsBuyerMaker,
		"isBestMatch":  trade.IsBestMatch,
	}
}

// accountToMap 将AccountResponse转换为map
func (b *Binance) accountToMap(account *AccountResponse) map[string]interface{} {
	return map[string]interface{}{
		"makerCommission":  account.MakerCommission,
		"takerCommission":  account.TakerCommission,
		"buyerCommission":  account.BuyerCommission,
		"sellerCommission": account.SellerCommission,
		"canTrade":         account.CanTrade,
		"canWithdraw":      account.CanWithdraw,
		"canDeposit":       account.CanDeposit,
		"updateTime":       account.UpdateTime,
		"accountType":      account.AccountType,
		"balances":         account.Balances,
		"permissions":      account.Permissions,
	}
}

// orderToMap 将OrderResponse转换为map
func (b *Binance) orderToMap(order *OrderResponse) map[string]interface{} {
	return map[string]interface{}{
		"symbol":                  order.Symbol,
		"orderId":                 order.OrderId,
		"orderListId":             order.OrderListId,
		"clientOrderId":           order.ClientOrderId,
		"price":                   order.Price,
		"origQty":                 order.OrigQty,
		"executedQty":             order.ExecutedQty,
		"cummulativeQuoteQty":     order.CummulativeQuoteQty,
		"status":                  order.Status,
		"timeInForce":             order.TimeInForce,
		"type":                    order.Type,
		"side":                    order.Side,
		"stopPrice":               order.StopPrice,
		"icebergQty":              order.IcebergQty,
		"time":                    order.Time,
		"updateTime":              order.UpdateTime,
		"isWorking":               order.IsWorking,
		"workingTime":             order.WorkingTime,
		"origQuoteOrderQty":       order.OrigQuoteOrderQty,
		"selfTradePreventionMode": order.SelfTradePreventionMode,
		"fills":                   order.Fills,
	}
}

// parseJSONString 解析JSON字符串
func (b *Binance) parseJSONString(jsonStr string, v interface{}) error {
	return json.Unmarshal([]byte(jsonStr), v)
}

// ========== 转换为Map的辅助方法 ==========

// myTradeToMap 将MyTradeResponse转换为map
func (b *Binance) myTradeToMap(trade *MyTradeResponse) map[string]interface{} {
	return map[string]interface{}{
		"symbol":          trade.Symbol,
		"id":              trade.Id,
		"orderId":         trade.OrderId,
		"orderListId":     trade.OrderListId,
		"price":           trade.Price,
		"qty":             trade.Qty,
		"quoteQty":        trade.QuoteQty,
		"commission":      trade.Commission,
		"commissionAsset": trade.CommissionAsset,
		"time":            trade.Time,
		"isBuyer":         trade.IsBuyer,
		"isMaker":         trade.IsMaker,
		"isBestMatch":     trade.IsBestMatch,
	}
}

// depositAddressToMap 将DepositAddressResponse转换为map
func (b *Binance) depositAddressToMap(deposit *DepositAddressResponse) map[string]interface{} {
	return map[string]interface{}{
		"address": deposit.Address,
		"coin":    deposit.Coin,
		"tag":     deposit.Tag,
		"url":     deposit.Url,
	}
}

// depositRecordToMap 将DepositRecord转换为map
func (b *Binance) depositRecordToMap(deposit *DepositRecord) map[string]interface{} {
	return map[string]interface{}{
		"id":            deposit.Id,
		"amount":        deposit.Amount,
		"coin":          deposit.Coin,
		"network":       deposit.Network,
		"status":        deposit.Status,
		"address":       deposit.Address,
		"addressTag":    deposit.AddressTag,
		"txId":          deposit.TxId,
		"insertTime":    deposit.InsertTime,
		"transferType":  deposit.TransferType,
		"unlockConfirm": deposit.UnlockConfirm,
		"confirmTimes":  deposit.ConfirmTimes,
	}
}

// withdrawToMap 将WithdrawResponse转换为map
func (b *Binance) withdrawToMap(withdraw *WithdrawResponse) map[string]interface{} {
	return map[string]interface{}{
		"id": withdraw.Id,
	}
}

// withdrawRecordToMap 将WithdrawRecord转换为map
func (b *Binance) withdrawRecordToMap(withdraw *WithdrawRecord) map[string]interface{} {
	return map[string]interface{}{
		"id":              withdraw.Id,
		"amount":          withdraw.Amount,
		"transactionFee":  withdraw.TransactionFee,
		"coin":            withdraw.Coin,
		"status":          withdraw.Status,
		"address":         withdraw.Address,
		"addressTag":      withdraw.AddressTag,
		"txId":            withdraw.TxId,
		"applyTime":       withdraw.ApplyTime,
		"network":         withdraw.Network,
		"transferType":    withdraw.TransferType,
		"withdrawOrderId": withdraw.WithdrawOrderId,
		"info":            withdraw.Info,
		"confirmNo":       withdraw.ConfirmNo,
		"walletType":      withdraw.WalletType,
		"txKey":           withdraw.TxKey,
		"completeTime":    withdraw.CompleteTime,
	}
}

// positionToMap 将FuturesPositionRisk转换为map
func (b *Binance) positionToMap(position *FuturesPositionRisk) map[string]interface{} {
	return map[string]interface{}{
		"symbol":           position.Symbol,
		"positionAmt":      position.PositionAmt,
		"entryPrice":       position.EntryPrice,
		"markPrice":        position.MarkPrice,
		"unRealizedProfit": position.UnRealizedProfit,
		"liquidationPrice": position.LiquidationPrice,
		"leverage":         position.Leverage,
		"maxNotionalValue": position.MaxNotionalValue,
		"marginType":       position.MarginType,
		"isolatedMargin":   position.IsolatedMargin,
		"isAutoAddMargin":  position.IsAutoAddMargin,
		"positionSide":     position.PositionSide,
		"notional":         position.Notional,
		"isolatedWallet":   position.IsolatedWallet,
		"updateTime":       position.UpdateTime,
		"bidNotional":      position.BidNotional,
		"askNotional":      position.AskNotional,
	}
}

// parseAccountBalance 解析账户余额数据 (WebSocket版本)
func (ws *BinanceWebSocket) parseAccountBalance(data map[string]interface{}) *ccxt.Account {
	return ws.exchange.parseAccountBalance(data)
}

// parseOrderUpdate 解析订单更新数据 (WebSocket版本)
func (ws *BinanceWebSocket) parseOrderUpdate(data map[string]interface{}) *ccxt.Order {
	return ws.exchange.parseOrderUpdate(data)
}

// parseAccountBalance 解析账户余额数据
func (b *Binance) parseAccountBalance(data map[string]interface{}) *ccxt.Account {
	// 从 outboundAccountPosition 消息解析
	if balancesData, ok := data["B"].([]interface{}); ok {
		balances := make(map[string]ccxt.Balance)
		free := make(map[string]float64)
		used := make(map[string]float64)
		total := make(map[string]float64)

		for _, balanceData := range balancesData {
			if balanceMap, ok := balanceData.(map[string]interface{}); ok {
				currency := b.SafeString(balanceMap, "a", "")
				if currency == "" {
					continue
				}

				freeAmount := b.SafeFloat(balanceMap, "f", 0)
				lockedAmount := b.SafeFloat(balanceMap, "l", 0)
				totalAmount := freeAmount + lockedAmount

				balances[currency] = ccxt.Balance{
					Free:  freeAmount,
					Used:  lockedAmount,
					Total: totalAmount,
				}

				free[currency] = freeAmount
				used[currency] = lockedAmount
				total[currency] = totalAmount
			}
		}

		return &ccxt.Account{
			Balances:  balances,
			Free:      free,
			Used:      used,
			Total:     total,
			Info:      data,
			Timestamp: b.SafeInt(data, "E", 0),
			Datetime:  b.ISO8601(b.SafeInt(data, "E", 0)),
		}
	}

	// 从 balanceUpdate 消息解析单个余额
	if currency := b.SafeString(data, "a", ""); currency != "" {
		balances := make(map[string]ccxt.Balance)
		free := make(map[string]float64)
		used := make(map[string]float64)
		total := make(map[string]float64)

		freeAmount := b.SafeFloat(data, "f", 0)
		lockedAmount := b.SafeFloat(data, "l", 0)
		totalAmount := freeAmount + lockedAmount

		balances[currency] = ccxt.Balance{
			Free:  freeAmount,
			Used:  lockedAmount,
			Total: totalAmount,
		}

		free[currency] = freeAmount
		used[currency] = lockedAmount
		total[currency] = totalAmount

		return &ccxt.Account{
			Balances:  balances,
			Free:      free,
			Used:      used,
			Total:     total,
			Info:      data,
			Timestamp: b.SafeInt(data, "E", 0),
			Datetime:  b.ISO8601(b.SafeInt(data, "E", 0)),
		}
	}

	return nil
}

// parseOrderUpdate 解析订单更新数据
func (b *Binance) parseOrderUpdate(data map[string]interface{}) *ccxt.Order {
	// 从 executionReport 消息解析
	symbol := b.SafeString(data, "s", "")
	if symbol == "" {
		return nil
	}

	// 订单状态映射
	statusMap := map[string]string{
		"NEW":              "open",
		"PARTIALLY_FILLED": "open",
		"FILLED":           "closed",
		"CANCELED":         "canceled",
		"REJECTED":         "rejected",
		"EXPIRED":          "expired",
	}

	orderStatus := b.SafeString(data, "X", "")
	status := statusMap[orderStatus]
	if status == "" {
		status = "open"
	}

	// 订单类型映射
	typeMap := map[string]string{
		"LIMIT":             "limit",
		"MARKET":            "market",
		"STOP_LOSS":         "stop",
		"STOP_LOSS_LIMIT":   "stop-limit",
		"TAKE_PROFIT":       "take-profit",
		"TAKE_PROFIT_LIMIT": "take-profit-limit",
	}

	orderType := b.SafeString(data, "o", "")
	orderTypeNormalized := typeMap[orderType]
	if orderTypeNormalized == "" {
		orderTypeNormalized = strings.ToLower(orderType)
	}

	// 订单方向
	side := strings.ToLower(b.SafeString(data, "S", ""))

	// 数量和价格
	amount := b.SafeFloat(data, "q", 0)
	filled := b.SafeFloat(data, "z", 0)
	remaining := amount - filled
	price := b.SafeFloat(data, "p", 0)
	average := b.SafeFloat(data, "ap", 0)

	// 费用信息
	fee := ccxt.Fee{}
	if commission := b.SafeFloat(data, "n", 0); commission > 0 {
		fee.Cost = commission
		fee.Currency = b.SafeString(data, "N", "")
	}

	return &ccxt.Order{
		ID:                 b.SafeString(data, "i", ""),
		ClientOrderId:      b.SafeString(data, "c", ""),
		Timestamp:          b.SafeInt(data, "T", 0),
		Datetime:           b.ISO8601(b.SafeInt(data, "T", 0)),
		LastTradeTimestamp: b.SafeInt(data, "T", 0),
		Symbol:             symbol,
		Type:               orderTypeNormalized,
		TimeInForce:        strings.ToLower(b.SafeString(data, "f", "")),
		Side:               side,
		Status:             status,
		Amount:             amount,
		Price:              price,
		Cost:               filled * average,
		Average:            average,
		Filled:             filled,
		Remaining:          remaining,
		Fee:                fee,
		Trades:             nil, // WebSocket不提供交易详情
		Info:               data,
	}
}
