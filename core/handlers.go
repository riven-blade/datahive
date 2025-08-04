package core

import (
	"context"
	"datahive/pkg/logger"
	"datahive/pkg/protocol"
	"datahive/server"

	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Handlers struct {
	dataHive *DataHive
}

func NewHandlers(d *DataHive) *Handlers {
	return &Handlers{
		dataHive: d,
	}
}

// HandleSubscribe 处理订阅请求
func (h *Handlers) HandleSubscribe(conn server.Connection, msg *protocol.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req protocol.SubscribeRequest
	if err := proto.Unmarshal(msg.Payload, &req); err != nil {
		logger.Ctx(ctx).Error("解析订阅请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 转换为内部订阅请求 - 支持多个symbols一起订阅
	subscribeReq := SubscriptionRequest{
		Exchange: req.Exchange,
		Market:   req.MarketType,
		Symbols:  req.Symbols,
		Events:   convertToEventTypes(&req),
		Options:  convertToOptions(&req),
	}

	// 执行订阅
	_, err := h.dataHive.Subscribe(ctx, subscribeReq)
	if err != nil {
		logger.Ctx(ctx).Error("订阅失败", zap.Error(err))
		return conn.SendError(msg.Id, 500, err.Error())
	}

	// 构建响应
	resp := &protocol.SubscribeResponse{
		Success: true,
		Message: fmt.Sprintf("successfully subscribed to %d symbols", len(req.Symbols)),
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	return conn.SendResponse(protocol.ActionSubscribe, msg.Id, respData)
}

// HandleUnsubscribe 处理取消订阅请求
func (h *Handlers) HandleUnsubscribe(conn server.Connection, msg *protocol.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req protocol.UnsubscribeRequest
	if err := proto.Unmarshal(msg.Payload, &req); err != nil {
		logger.Ctx(ctx).Error("解析取消订阅请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 检查参数
	if len(req.Symbols) == 0 {
		return conn.SendError(msg.Id, 400, "no symbols provided")
	}

	// 转换为内部取消订阅请求
	unsubscribeReq := SubscriptionRequest{
		Exchange: req.Exchange,
		Market:   req.MarketType,
		Symbols:  req.Symbols,
		Events: convertToEventTypes(&protocol.SubscribeRequest{
			Ticker:    req.Ticker,
			Kline:     req.Kline,
			Trade:     req.Trade,
			Orderbook: req.Orderbook,
		}),
	}

	// 执行取消订阅
	err := h.dataHive.UnsubscribeByRequest(ctx, unsubscribeReq)
	if err != nil {
		logger.Ctx(ctx).Error("取消订阅失败", zap.Error(err))
		return conn.SendError(msg.Id, 500, err.Error())
	}

	// 构建响应
	resp := &protocol.UnsubscribeResponse{
		Success: true,
		Message: fmt.Sprintf("successfully unsubscribed from %d symbols", len(req.Symbols)),
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	return conn.SendResponse(protocol.ActionUnsubscribe, msg.Id, respData)
}

// HandleFetchMarkets 处理获取市场数据请求
func (h *Handlers) HandleFetchMarkets(conn server.Connection, msg *protocol.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req protocol.FetchMarketsRequest
	if err := proto.Unmarshal(msg.Payload, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取市场请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 实际调用DataHive获取交易所市场数据
	markets, err := h.dataHive.FetchMarkets(ctx, req.Exchange, req.MarketType)
	if err != nil {
		logger.Ctx(ctx).Error("获取市场数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("market_type", req.MarketType),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch markets: %v", err))
	}

	// 转换ccxt.Market到protocol.Market
	protocolMarkets := make([]*protocol.Market, 0, len(markets))
	for _, market := range markets {
		protocolMarket := &protocol.Market{
			Id:       market.ID,
			Symbol:   market.Symbol,
			Base:     market.Base,
			Quote:    market.Quote,
			Active:   market.Active,
			Type:     market.Type,
			Spot:     market.Spot,
			Futures:  market.Future, // Future -> Futures
			Swap:     market.Swap,
			Linear:   market.Linear,
			Inverse:  market.Inverse,
			MakerFee: market.Maker, // Maker -> MakerFee
			TakerFee: market.Taker, // Taker -> TakerFee
		}
		protocolMarkets = append(protocolMarkets, protocolMarket)
	}

	resp := &protocol.FetchMarketsResponse{
		Markets: protocolMarkets,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	logger.Ctx(ctx).Info("返回市场数据",
		zap.String("exchange", req.Exchange),
		zap.String("market_type", req.MarketType),
		zap.Int("count", len(protocolMarkets)))

	return conn.SendResponse(protocol.ActionFetchMarkets, msg.Id, respData)
}

// HandleFetchTicker 处理获取ticker数据请求
func (h *Handlers) HandleFetchTicker(conn server.Connection, msg *protocol.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req protocol.FetchTickerRequest
	if err := proto.Unmarshal(msg.Payload, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取ticker请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 实际调用DataHive获取ticker数据 (使用默认market type)
	ticker, err := h.dataHive.FetchTicker(ctx, req.Exchange, "spot", req.Symbol)
	if err != nil {
		logger.Ctx(ctx).Error("获取ticker数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch ticker: %v", err))
	}

	// 转换ccxt.Ticker到protocol.Ticker
	protocolTicker := &protocol.Ticker{
		Symbol:        ticker.Symbol,
		Timestamp:     ticker.TimeStamp,
		High:          ticker.High,
		Low:           ticker.Low,
		Bid:           ticker.Bid,
		Ask:           ticker.Ask,
		Open:          ticker.Open,
		Close:         ticker.Close,
		Last:          ticker.Last,
		Volume:        ticker.BaseVolume,
		Change:        ticker.Change,
		ChangePercent: ticker.Percentage,
	}

	resp := &protocol.FetchTickerResponse{
		Ticker: protocolTicker,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	logger.Ctx(ctx).Info("返回ticker数据",
		zap.String("exchange", req.Exchange),
		zap.String("symbol", req.Symbol))

	return conn.SendResponse(protocol.ActionFetchTicker, msg.Id, respData)
}

// HandleFetchKlines 处理获取K线数据请求
func (h *Handlers) HandleFetchKlines(conn server.Connection, msg *protocol.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req protocol.FetchKlinesRequest
	if err := proto.Unmarshal(msg.Payload, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取K线请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 实际调用DataHive获取OHLCV数据 (使用默认market type和StartTime作为since参数)
	ohlcvList, err := h.dataHive.FetchOHLCV(ctx,
		req.Exchange, "spot", req.Symbol, req.Interval, req.StartTime, int(req.Limit))
	if err != nil {
		logger.Ctx(ctx).Error("获取K线数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.String("interval", req.Interval),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch klines: %v", err))
	}

	// 转换ccxt.OHLCV到protocol.Kline
	protocolKlines := make([]*protocol.Kline, 0, len(ohlcvList))
	for _, ohlcv := range ohlcvList {
		protocolKline := &protocol.Kline{
			Symbol:    req.Symbol,
			Exchange:  req.Exchange,
			Timeframe: req.Interval,
			OpenTime:  ohlcv.Timestamp,
			Open:      ohlcv.Open,
			High:      ohlcv.High,
			Low:       ohlcv.Low,
			Close:     ohlcv.Close,
			Volume:    ohlcv.Volume,
			Closed:    true, // 假设都是已收盘的K线
		}
		protocolKlines = append(protocolKlines, protocolKline)
	}

	resp := &protocol.FetchKlinesResponse{
		Klines: protocolKlines,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	logger.Ctx(ctx).Info("返回K线数据",
		zap.String("exchange", req.Exchange),
		zap.String("symbol", req.Symbol),
		zap.String("interval", req.Interval),
		zap.Int("count", len(protocolKlines)))

	return conn.SendResponse(protocol.ActionFetchKlines, msg.Id, respData)
}

// HandleFetchOrderBook 处理获取订单簿数据请求
func (h *Handlers) HandleFetchOrderBook(conn server.Connection, msg *protocol.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req protocol.FetchOrderBookRequest
	if err := proto.Unmarshal(msg.Payload, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取订单簿请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 实际调用DataHive获取订单簿数据 (使用默认market type)
	orderBook, err := h.dataHive.FetchOrderBook(ctx,
		req.Exchange, "spot", req.Symbol, int(req.Limit))
	if err != nil {
		logger.Ctx(ctx).Error("获取订单簿数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch order book: %v", err))
	}

	// 转换ccxt.OrderBook到protocol.OrderBook
	protocolOrderBook := &protocol.OrderBook{
		Symbol:    orderBook.Symbol,
		Timestamp: orderBook.TimeStamp,
		Bids:      make([]*protocol.PriceLevel, 0, len(orderBook.Bids.Price)),
		Asks:      make([]*protocol.PriceLevel, 0, len(orderBook.Asks.Price)),
	}

	// 转换买单
	for i := 0; i < len(orderBook.Bids.Price) && i < len(orderBook.Bids.Size); i++ {
		bid := &protocol.PriceLevel{
			Price:    orderBook.Bids.Price[i],
			Quantity: orderBook.Bids.Size[i],
		}
		protocolOrderBook.Bids = append(protocolOrderBook.Bids, bid)
	}

	// 转换卖单
	for i := 0; i < len(orderBook.Asks.Price) && i < len(orderBook.Asks.Size); i++ {
		ask := &protocol.PriceLevel{
			Price:    orderBook.Asks.Price[i],
			Quantity: orderBook.Asks.Size[i],
		}
		protocolOrderBook.Asks = append(protocolOrderBook.Asks, ask)
	}

	resp := &protocol.FetchOrderBookResponse{
		Orderbook: protocolOrderBook,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	logger.Ctx(ctx).Info("返回订单簿数据",
		zap.String("exchange", req.Exchange),
		zap.String("symbol", req.Symbol),
		zap.Int("bids", len(protocolOrderBook.Bids)),
		zap.Int("asks", len(protocolOrderBook.Asks)))

	return conn.SendResponse(protocol.ActionFetchOrderBook, msg.Id, respData)
}

// HandleHealth 处理健康检查请求
func (h *Handlers) HandleHealth(conn server.Connection, msg *protocol.Message) error {
	health := h.dataHive.Health()

	// 简化健康检查响应，直接返回JSON字符串
	respData := fmt.Sprintf(`{"status":"%s","timestamp":%d}`, health.Status, health.Timestamp)

	return conn.SendResponse(protocol.ActionSubscribe, msg.Id, []byte(respData))
}

// =============================================================================
// 辅助函数
// =============================================================================

// convertToEventTypes 转换协议请求到事件类型
func convertToEventTypes(req *protocol.SubscribeRequest) []EventType {
	var events []EventType

	if req.Ticker {
		events = append(events, EventTicker)
	}
	if req.Kline {
		events = append(events, EventKline)
	}
	if req.Trade {
		events = append(events, EventTrade)
	}
	if req.Orderbook {
		events = append(events, EventOrderBook)
	}

	return events
}

// convertToOptions 转换协议请求到选项
func convertToOptions(req *protocol.SubscribeRequest) Options {
	return Options{
		Interval: req.KlineInterval,
		Depth:    int(req.OrderbookDepth),
	}
}
