package bybit

import (
	"strconv"
	"strings"
	"time"

	"github.com/riven-blade/datahive/pkg/ccxt"
)

// ========== 市场数据解析器 ==========

// parseMarket 解析市场信息
func (b *Bybit) parseMarket(data map[string]interface{}) *ccxt.Market {
	symbol, _ := data["symbol"].(string)
	if symbol == "" {
		return nil
	}

	status, _ := data["status"].(string)
	active := status == "Trading"

	baseCoin, _ := data["baseCoin"].(string)
	quoteCoin, _ := data["quoteCoin"].(string)
	settleCoin, _ := data["settleCoin"].(string)

	contractType, _ := data["contractType"].(string)
	marketType := b.parseMarketType(contractType)
	unifiedSymbol := b.parseSymbol(symbol, baseCoin, quoteCoin, settleCoin, marketType)

	// 解析精度
	priceFilter, _ := data["priceFilter"].(map[string]interface{})
	lotSizeFilter, _ := data["lotSizeFilter"].(map[string]interface{})

	precision := ccxt.MarketPrecision{
		Amount: b.parsePrecision(lotSizeFilter, "qtyStep"),
		Price:  b.parsePrecision(priceFilter, "tickSize"),
		Cost:   8, // 默认值
	}

	// 解析限制
	limits := ccxt.MarketLimits{
		Amount: ccxt.LimitRange{
			Min: b.parseFloat(lotSizeFilter, "minOrderQty"),
			Max: b.parseFloat(lotSizeFilter, "maxOrderQty"),
		},
		Price: ccxt.LimitRange{
			Min: b.parseFloat(priceFilter, "minPrice"),
			Max: b.parseFloat(priceFilter, "maxPrice"),
		},
		Cost: ccxt.LimitRange{
			Min: 0,
			Max: 0,
		},
		Leverage: ccxt.LimitRange{
			Min: 1,
			Max: b.parseFloat(data, "maxLeverage"),
		},
	}

	// 使用默认值，实际项目中应从API获取或配置
	var maker, taker float64 = 0.0001, 0.0006

	return &ccxt.Market{
		ID:             symbol,
		Symbol:         unifiedSymbol,
		Base:           baseCoin,
		Quote:          quoteCoin,
		Settle:         settleCoin,
		Type:           marketType,
		Spot:           marketType == ccxt.MarketTypeSpot,
		Margin:         false, // Bybit现货保证金支持有限
		Future:         marketType == ccxt.MarketTypeFuture,
		Option:         marketType == ccxt.MarketTypeOption,
		Active:         active,
		Contract:       marketType != ccxt.MarketTypeSpot,
		Linear:         contractType == ccxt.ContractTypeLinear,
		Inverse:        contractType == ccxt.ContractTypeInverse,
		Taker:          taker,
		Maker:          maker,
		ContractSize:   1.0, // 默认值，可能需要从API获取
		Expiry:         0,   // 现货和永续合约没有到期时间
		ExpiryDatetime: "",
		Strike:         0,
		OptionType:     "",
		Precision:      precision,
		Limits:         limits,
		Info:           data,
	}
}

// parseTicker 解析ticker数据
func (b *Bybit) parseTicker(data map[string]interface{}, market *ccxt.Market) *ccxt.Ticker {
	symbol, _ := data["symbol"].(string)
	if symbol == "" || market == nil {
		return nil
	}

	timestamp := time.Now().UnixMilli()
	if ts, ok := data["time"].(float64); ok {
		timestamp = int64(ts)
	}

	lastPrice := b.parseFloat(data, "lastPrice")
	bid := b.parseFloat(data, "bid1Price")
	ask := b.parseFloat(data, "ask1Price")
	bidVolume := b.parseFloat(data, "bid1Size")
	askVolume := b.parseFloat(data, "ask1Size")

	open := b.parseFloat(data, "prevPrice24h")
	high := b.parseFloat(data, "highPrice24h")
	low := b.parseFloat(data, "lowPrice24h")
	volume := b.parseFloat(data, "volume24h")
	quoteVolume := b.parseFloat(data, "turnover24h")

	var change, percentage float64
	if open > 0 && lastPrice > 0 {
		change = lastPrice - open
		percentage = (change / open) * 100
	}

	if changeStr, ok := data["price24hPcnt"].(string); ok {
		if p, err := strconv.ParseFloat(changeStr, 64); err == nil {
			percentage = p * 100 // Bybit返回的是小数，需要转换为百分比
		}
	}

	return &ccxt.Ticker{
		Symbol:        market.Symbol,
		TimeStamp:     timestamp,
		Datetime:      time.Unix(timestamp/1000, 0).UTC().Format(time.RFC3339),
		High:          high,
		Low:           low,
		Bid:           bid,
		BidVolume:     bidVolume,
		Ask:           ask,
		AskVolume:     askVolume,
		Vwap:          0, // Bybit不直接提供VWAP
		Open:          open,
		Close:         lastPrice,
		Last:          lastPrice,
		PreviousClose: 0,
		Change:        change,
		Percentage:    percentage,
		Average:       (lastPrice + open) / 2,
		BaseVolume:    volume,
		QuoteVolume:   quoteVolume,
		Info:          data,
	}
}

// parseKline 解析K线数据
func (b *Bybit) parseKline(data []interface{}, market *ccxt.Market) *ccxt.OHLCV {
	if len(data) < 7 {
		return nil
	}

	// Bybit K线格式: [startTime, open, high, low, close, volume, turnover]
	timestamp := b.parseIntFromInterface(data[0])
	open := b.parseFloatFromInterface(data[1])
	high := b.parseFloatFromInterface(data[2])
	low := b.parseFloatFromInterface(data[3])
	close := b.parseFloatFromInterface(data[4])
	volume := b.parseFloatFromInterface(data[5])

	return &ccxt.OHLCV{
		Timestamp: timestamp,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
	}
}

// parseTrade 解析交易数据
func (b *Bybit) parseTrade(data map[string]interface{}, market *ccxt.Market) *ccxt.Trade {
	id, _ := data["execId"].(string)
	if id == "" {
		return nil
	}

	timestamp := b.parseIntFromMap(data, "time")
	side, _ := data["side"].(string)
	amount := b.parseFloat(data, "execQty")
	price := b.parseFloat(data, "execPrice")
	cost := amount * price

	return &ccxt.Trade{
		ID:           id,
		Info:         data,
		Timestamp:    timestamp,
		Datetime:     b.iso8601(timestamp),
		Symbol:       market.Symbol,
		Type:         ccxt.OrderTypeMarket, // 公开交易通常是市价成交
		Side:         side,
		Amount:       amount,
		Price:        price,
		Cost:         cost,
		Fee:          ccxt.Fee{}, // 公开交易数据不包含费用信息
		TakerOrMaker: "",
	}
}

// parseOrderBook 解析订单簿
func (b *Bybit) parseOrderBook(data map[string]interface{}, market *ccxt.Market) *ccxt.OrderBook {
	timestamp := time.Now().UnixMilli()
	if ts, ok := data["ts"].(string); ok {
		if t, err := strconv.ParseInt(ts, 10, 64); err == nil {
			timestamp = t
		}
	}

	bidsData, _ := data["b"].([]interface{})
	asksData, _ := data["a"].([]interface{})

	// 解析买单
	bidPrices := make([]float64, 0)
	bidSizes := make([]float64, 0)
	for _, bidData := range bidsData {
		if bidArray, ok := bidData.([]interface{}); ok && len(bidArray) >= 2 {
			price := b.parseFloatFromInterface(bidArray[0])
			amount := b.parseFloatFromInterface(bidArray[1])
			if price > 0 && amount > 0 {
				bidPrices = append(bidPrices, price)
				bidSizes = append(bidSizes, amount)
			}
		}
	}

	// 解析卖单
	askPrices := make([]float64, 0)
	askSizes := make([]float64, 0)
	for _, askData := range asksData {
		if askArray, ok := askData.([]interface{}); ok && len(askArray) >= 2 {
			price := b.parseFloatFromInterface(askArray[0])
			amount := b.parseFloatFromInterface(askArray[1])
			if price > 0 && amount > 0 {
				askPrices = append(askPrices, price)
				askSizes = append(askSizes, amount)
			}
		}
	}

	return &ccxt.OrderBook{
		Symbol:    market.Symbol,
		Bids:      ccxt.OrderBookSide{Price: bidPrices, Size: bidSizes},
		Asks:      ccxt.OrderBookSide{Price: askPrices, Size: askSizes},
		TimeStamp: timestamp,
		Datetime:  time.Unix(timestamp/1000, 0).UTC().Format(time.RFC3339),
		Nonce:     0,
		Info:      data,
	}
}

// ========== 账户数据解析器 ==========

// parseBalance 解析余额数据
func (b *Bybit) parseBalance(data map[string]interface{}) *ccxt.Account {
	account := &ccxt.Account{
		Info:      data,
		Timestamp: time.Now().UnixMilli(),
		Free:      make(map[string]float64),
		Used:      make(map[string]float64),
		Total:     make(map[string]float64),
	}

	account.Datetime = time.Unix(account.Timestamp/1000, 0).UTC().Format(time.RFC3339)

	coinData, ok := data["coin"].([]interface{})
	if !ok {
		return account
	}

	for _, coinInfo := range coinData {
		coin, ok := coinInfo.(map[string]interface{})
		if !ok {
			continue
		}

		currency, _ := coin["coin"].(string)
		if currency == "" {
			continue
		}

		free := b.parseFloat(coin, "availableToWithdraw")
		locked := b.parseFloat(coin, "locked")
		total := b.parseFloat(coin, "walletBalance")

		if total > 0 || free > 0 || locked > 0 {
			account.Free[currency] = free
			account.Used[currency] = locked
			account.Total[currency] = total
		}
	}

	return account
}

// ========== 交易数据解析器 ==========

// parseOrder 解析订单数据
func (b *Bybit) parseOrder(data map[string]interface{}, market *ccxt.Market) *ccxt.Order {
	id, _ := data["orderId"].(string)
	if id == "" {
		return nil
	}

	clientOrderId, _ := data["orderLinkId"].(string)
	timestamp := b.parseIntFromMap(data, "createdTime")
	lastTradeTimestamp := b.parseIntFromMap(data, "updatedTime")

	orderType := b.parseOrderType(data)
	side, _ := data["side"].(string)
	amount := b.parseFloat(data, "qty")
	price := b.parseFloat(data, "price")
	filled := b.parseFloat(data, "cumExecQty")
	remaining := amount - filled
	average := 0.0
	if filled > 0 {
		average = b.parseFloat(data, "cumExecValue") / filled
	}
	cost := filled * average
	status := b.parseOrderStatus(data)

	return &ccxt.Order{
		ID:                 id,
		ClientOrderId:      clientOrderId, // 使用正确的字段名
		Info:               data,
		Timestamp:          timestamp,
		Datetime:           b.iso8601(timestamp),
		LastTradeTimestamp: lastTradeTimestamp,
		Symbol:             market.Symbol,
		Type:               orderType,
		TimeInForce:        b.parseTimeInForce(data),
		Side:               side,
		Amount:             amount,
		Price:              price,
		StopPrice:          b.parseFloat(data, "triggerPrice"),
		Average:            average,
		Filled:             filled,
		Remaining:          remaining,
		Status:             status,
		Fee:                ccxt.Fee{}, // 使用空结构体而不是nil
		Cost:               cost,
		Trades:             []ccxt.Trade{},
	}
}

// parsePosition 解析持仓数据
func (b *Bybit) parsePosition(data map[string]interface{}) *ccxt.Position {
	symbol, _ := data["symbol"].(string)
	if symbol == "" {
		return nil
	}

	side, _ := data["side"].(string)
	size := b.parseFloat(data, "size")

	// 如果持仓为0，跳过
	if size == 0 {
		return nil
	}

	contracts := size
	contractSize := 1.0 // Bybit默认合约大小为1
	notional := b.parseFloat(data, "positionValue")

	entryPrice := b.parseFloat(data, "avgPrice")
	markPrice := b.parseFloat(data, "markPrice")
	unrealizedPnl := b.parseFloat(data, "unrealisedPnl")
	realizedPnl := b.parseFloat(data, "cumRealisedPnl")

	leverage := b.parseFloat(data, "leverage")

	// 转换方向
	side = strings.ToLower(side)
	if side == "none" {
		side = ""
	}

	timestamp := b.parseIntFromMap(data, "updatedTime")

	return &ccxt.Position{
		Info:              data,
		ID:                "",
		Symbol:            symbol, // 需要转换为统一格式
		Timestamp:         timestamp,
		Datetime:          time.Unix(timestamp/1000, 0).UTC().Format(time.RFC3339),
		Contracts:         contracts,
		ContractSize:      contractSize,
		Side:              side,
		Size:              size,
		NotionalValue:     notional,
		EntryPrice:        entryPrice,
		MarkPrice:         markPrice,
		UnrealizedPnl:     unrealizedPnl,
		RealizedPnl:       realizedPnl,
		Leverage:          leverage,
		Collateral:        0, // 需要计算
		InitialMargin:     b.parseFloat(data, "positionIM"),
		MaintenanceMargin: b.parseFloat(data, "positionMM"),
		MarginRatio:       0, // 需要计算
		LiquidationPrice:  b.parseFloat(data, "liqPrice"),
	}
}

// ========== 辅助解析方法 ==========

// parseFloat 从map中解析float64
func (b *Bybit) parseFloat(data map[string]interface{}, key string) float64 {
	if value, ok := data[key]; ok {
		switch v := value.(type) {
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

// parseFloatFromInterface 从interface{}中解析float64
func (b *Bybit) parseFloatFromInterface(value interface{}) float64 {
	switch v := value.(type) {
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return 0
}

// parseIntFromMap 从map中解析int64时间戳
func (b *Bybit) parseIntFromMap(data map[string]interface{}, key string) int64 {
	if value, ok := data[key]; ok {
		switch v := value.(type) {
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		case float64:
			return int64(v)
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 0
}

// parseIntFromInterface 从interface{}中解析int64
func (b *Bybit) parseIntFromInterface(value interface{}) int64 {
	switch v := value.(type) {
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}
	return 0
}

// parsePrecision 解析精度（修复返回类型）
func (b *Bybit) parsePrecision(filter map[string]interface{}, key string) float64 {
	if filter == nil {
		return 8.0
	}

	stepStr, ok := filter[key].(string)
	if !ok {
		return 8.0
	}

	// 计算小数点位数
	if stepStr == "" || stepStr == "0" {
		return 8.0
	}

	// 简单计算：找到第一个非零数字的位置
	decimalIndex := strings.Index(stepStr, ".")
	if decimalIndex == -1 {
		return 0.0
	}

	precision := 0.0
	for i := decimalIndex + 1; i < len(stepStr); i++ {
		if stepStr[i] == '0' {
			precision++
		} else {
			precision++
			break
		}
	}

	return precision
}

// parseOrderStatus 解析订单状态
func (b *Bybit) parseOrderStatus(data map[string]interface{}) string {
	status, _ := data["orderStatus"].(string)
	switch status {
	case "New", "PartiallyFilled":
		return ccxt.OrderStatusOpen
	case "Filled":
		return ccxt.OrderStatusClosed
	case "Cancelled":
		return ccxt.OrderStatusCanceled
	case "Rejected":
		return ccxt.OrderStatusRejected
	default:
		return ccxt.OrderStatusOpen
	}
}

// parseTimeInForce 解析订单有效期
func (b *Bybit) parseTimeInForce(data map[string]interface{}) string {
	tif, _ := data["timeInForce"].(string)
	switch tif {
	case "GTC":
		return "GTC"
	case "IOC":
		return "IOC"
	case "FOK":
		return "FOK"
	default:
		return "GTC"
	}
}

// parseMarketType 解析市场类型
func (b *Bybit) parseMarketType(contractType string) string {
	switch contractType {
	case "spot":
		return ccxt.MarketTypeSpot
	case "linearPerpetual", "linearFutures":
		return ccxt.MarketTypeFuture
	case "inversePerpetual", "inverseFutures":
		return ccxt.MarketTypeFuture
	case "option":
		return ccxt.MarketTypeOption
	default:
		return ccxt.MarketTypeSpot
	}
}

// parseSymbol 解析统一符号
func (b *Bybit) parseSymbol(symbol, base, quote, settle, marketType string) string {
	if marketType == ccxt.MarketTypeSpot {
		return base + "/" + quote
	}
	if settle != "" && settle != quote {
		return base + "/" + quote + ":" + settle
	}
	return base + "/" + quote
}

// iso8601 时间格式化
func (b *Bybit) iso8601(timestamp int64) string {
	if timestamp == 0 {
		return ""
	}
	return time.Unix(timestamp/1000, 0).UTC().Format("2006-01-02T15:04:05.000Z")
}

// parseOrderType 解析订单类型
func (b *Bybit) parseOrderType(data map[string]interface{}) string {
	orderType, _ := data["orderType"].(string)
	switch orderType {
	case "Market":
		return ccxt.OrderTypeMarket
	case "Limit":
		return ccxt.OrderTypeLimit
	default:
		return ccxt.OrderTypeLimit
	}
}
