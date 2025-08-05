package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/riven-blade/datahive/pkg/ccxt"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol/pb"
	"github.com/riven-blade/datahive/pkg/server"
	"github.com/riven-blade/datahive/pkg/utils"

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

// HandleSubscribe 处理订阅请求 - 支持单个币种和单个类型
func (h *Handlers) HandleSubscribe(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background()

	var req pb.SubscribeRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析订阅请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 验证请求参数
	if req.Exchange == "" || req.MarketType == "" || req.Symbol == "" {
		return conn.SendError(msg.Id, 400, "exchange, market_type and symbol are required")
	}

	// 转换数据类型
	eventType, err := convertDataTypeToEventType(req.DataType)
	if err != nil {
		logger.Ctx(ctx).Error("不支持的数据类型", zap.String("data_type", req.DataType.String()))
		return conn.SendError(msg.Id, 400, fmt.Sprintf("unsupported data type: %s", req.DataType.String()))
	}

	subscribeReq := SubscriptionRequest{
		Exchange: req.Exchange,
		Market:   req.MarketType,
		Symbol:   req.Symbol, // 单个符号
		Event:    eventType,  // 单个事件类型
		Options:  convertToOptionsFromNewReq(&req),
	}

	topics, err := h.dataHive.Subscribe(ctx, subscribeReq)
	if err != nil {
		logger.Ctx(ctx).Error("订阅失败", zap.Error(err))
		return conn.SendError(msg.Id, 500, err.Error())
	}

	// 由于是单个订阅，topics数组应该只有一个元素
	if len(topics) != 1 {
		logger.Ctx(ctx).Error("订阅返回topic数量异常", zap.Int("count", len(topics)))
		return conn.SendError(msg.Id, 500, "unexpected topic count")
	}

	topic := topics[0]

	// client server 端订阅
	if err := conn.Subscribe(topic); err != nil {
		logger.Ctx(ctx).Error("订阅topic失败",
			zap.String("topic", topic),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to subscribe topic: %s", err.Error()))
	}

	logger.Ctx(ctx).Info("订阅topic成功",
		zap.String("topic", topic),
		zap.String("symbol", req.Symbol),
		zap.String("data_type", req.DataType.String()),
		zap.String("remote", conn.GetRemote()))

	// 构建响应 - 包含生成的topic
	resp := &pb.SubscribeResponse{
		Success: true,
		Message: fmt.Sprintf("successfully subscribed to %s %s", req.Symbol, req.DataType.String()),
		Topic:   topic, // 返回生成的topic
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	return conn.SendResponse(pb.ActionType_SUBSCRIBE, msg.Id, respData)
}

// HandleUnsubscribe 处理取消订阅请求 - 简化为直接使用topic
func (h *Handlers) HandleUnsubscribe(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background()

	var req pb.UnsubscribeRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析取消订阅请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 验证参数
	if req.Topic == "" {
		return conn.SendError(msg.Id, 400, "topic is required")
	}

	// 从连接上取消订阅
	if err := conn.Unsubscribe(req.Topic); err != nil {
		logger.Ctx(ctx).Error("取消订阅topic失败",
			zap.String("topic", req.Topic),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to unsubscribe topic: %s", err.Error()))
	}

	logger.Ctx(ctx).Info("取消订阅topic成功",
		zap.String("topic", req.Topic),
		zap.String("remote", conn.GetRemote()))

	// 构建响应
	resp := &pb.UnsubscribeResponse{
		Success: true,
		Message: fmt.Sprintf("successfully unsubscribed from topic: %s", req.Topic),
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	return conn.SendResponse(pb.ActionType_UNSUBSCRIBE, msg.Id, respData)
}

// HandleFetchMarkets 处理获取市场数据请求
func (h *Handlers) HandleFetchMarkets(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background() // 创建context用于日志

	// 添加调试信息
	logger.Ctx(ctx).Debug("收到FetchMarkets请求",
		zap.String("action", msg.Action.String()),
		zap.String("id", msg.Id),
		zap.Int("payload_len", len(msg.Data)),
		zap.String("payload_hex", fmt.Sprintf("%x", msg.Data[:minInt(32, len(msg.Data))]))) // 只显示前32字节的hex

	var req pb.FetchMarketsRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取市场请求失败",
			zap.Error(err),
			zap.Int("payload_len", len(msg.Data)),
			zap.String("payload_hex", fmt.Sprintf("%x", msg.Data)))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	logger.Ctx(ctx).Debug("成功解析FetchMarkets请求",
		zap.String("exchange", req.Exchange),
		zap.String("market_type", req.MarketType))

	markets, err := h.dataHive.FetchMarkets(ctx, req.Exchange, req.MarketType)
	if err != nil {
		logger.Ctx(ctx).Error("获取市场数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("market_type", req.MarketType),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch markets: %v", err))
	}

	// 根据 MarketType 和 StackType 进行筛选
	logger.Ctx(ctx).Debug("开始筛选市场",
		zap.Int("before_filter_count", len(markets)),
		zap.String("market_type", req.MarketType),
		zap.String("stack_type", req.StackType))

	markets = filterMarkets(markets, req.MarketType, req.StackType)

	logger.Ctx(ctx).Debug("筛选后市场数量",
		zap.Int("after_filter_count", len(markets)))

	// 转换ccxt.Market到pb.Market
	protocolMarkets := make([]*pb.Market, 0, len(markets))
	for _, market := range markets {
		protocolMarket := convertCCXTMarketToProtocol(market)
		protocolMarkets = append(protocolMarkets, protocolMarket)
	}

	resp := &pb.FetchMarketsResponse{
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

	return conn.SendResponse(pb.ActionType_FETCH_MARKETS, msg.Id, respData)
}

// HandleFetchTicker 处理获取ticker数据请求
func (h *Handlers) HandleFetchTicker(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req pb.FetchTickerRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取ticker请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	ticker, err := h.dataHive.FetchTicker(ctx, req.Exchange, req.MarketType, req.Symbol)
	if err != nil {
		logger.Ctx(ctx).Error("获取ticker数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch ticker: %v", err))
	}

	protocolTicker := &pb.Ticker{
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

	logger.Ctx(ctx).Debug("转换后的ticker数据",
		zap.String("cleaned_symbol", ticker.Symbol),
		zap.Bool("protocol_symbol_valid_utf8", utils.IsValidUTF8(protocolTicker.Symbol)))

	resp := &pb.FetchTickerResponse{
		Ticker: protocolTicker,
	}

	// 在序列化前验证所有字符串字段
	logger.Ctx(ctx).Debug("准备序列化FetchTickerResponse",
		zap.String("ticker_symbol", resp.Ticker.Symbol),
		zap.Bool("response_ticker_symbol_valid_utf8", utils.IsValidUTF8(resp.Ticker.Symbol)),
		zap.String("message_id", msg.Id),
		zap.Bool("message_id_valid_utf8", utils.IsValidUTF8(msg.Id)))

	respData, err := proto.Marshal(resp)
	if err != nil {
		logger.Ctx(ctx).Error("protobuf序列化失败",
			zap.Error(err),
			zap.String("ticker_symbol", resp.Ticker.Symbol))
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	// 验证序列化后的数据
	logger.Ctx(ctx).Debug("protobuf序列化成功",
		zap.Int("response_data_length", len(respData)),
		zap.Bool("response_data_valid_utf8", utils.IsValidUTF8(string(respData))))

	logger.Ctx(ctx).Info("返回ticker数据",
		zap.String("exchange", req.Exchange),
		zap.String("symbol", req.Symbol))

	return conn.SendResponse(pb.ActionType_FETCH_TICKER, msg.Id, respData)
}

// HandleFetchTickers 处理获取多个ticker请求
func (h *Handlers) HandleFetchTickers(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req pb.FetchTickersRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取多个ticker请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	if req.Symbol == "" {
		return conn.SendError(msg.Id, 400, "symbol cannot be empty")
	}

	// 获取单个ticker数据
	ticker, err := h.dataHive.FetchTicker(ctx, req.Exchange, req.MarketType, req.Symbol)
	if err != nil {
		logger.Ctx(ctx).Error("获取ticker失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.Error(err))
		return conn.SendError(msg.Id, 404, fmt.Sprintf("failed to fetch ticker for %s", req.Symbol))
	}

	// 验证UTF-8编码并添加调试日志
	logger.Ctx(ctx).Debug("检查ticker字段UTF-8编码",
		zap.String("symbol", req.Symbol),
		zap.String("original_symbol", ticker.Symbol),
		zap.String("original_datetime", ticker.Datetime),
		zap.Bool("symbol_valid_utf8", utils.IsValidUTF8(ticker.Symbol)),
		zap.Bool("datetime_valid_utf8", utils.IsValidUTF8(ticker.Datetime)))

	protocolTicker := &pb.Ticker{
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

	resp := &pb.FetchTickersResponse{
		Ticker: protocolTicker,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	logger.Ctx(ctx).Info("返回ticker数据",
		zap.String("exchange", req.Exchange),
		zap.String("symbol", req.Symbol))

	return conn.SendResponse(pb.ActionType_FETCH_TICKERS, msg.Id, respData)
}

// HandleFetchKlines 处理获取K线数据请求
func (h *Handlers) HandleFetchKlines(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background()

	var req pb.FetchKlinesRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取K线请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	ohlcvList, err := h.dataHive.FetchOHLCV(ctx, req.Exchange, req.MarketType, req.Symbol,
		req.Interval, req.StartTime, int(req.Limit))
	if err != nil {
		logger.Ctx(ctx).Error("获取K线数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.String("interval", req.Interval),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch klines: %v", err))
	}

	protocolKlines := make([]*pb.Kline, 0, len(ohlcvList))
	for _, ohlcv := range ohlcvList {
		protocolKline := &pb.Kline{
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

	resp := &pb.FetchKlinesResponse{
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

	return conn.SendResponse(pb.ActionType_FETCH_KLINES, msg.Id, respData)
}

// HandleFetchOrderBook 处理获取订单簿数据请求
func (h *Handlers) HandleFetchOrderBook(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req pb.FetchOrderBookRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取订单簿请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	orderBook, err := h.dataHive.FetchOrderBook(ctx,
		req.Exchange, req.MarketType, req.Symbol, int(req.Limit))
	if err != nil {
		logger.Ctx(ctx).Error("获取订单簿数据失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch order book: %v", err))
	}

	// 转换ccxt.OrderBook到pb.OrderBook
	protocolOrderBook := &pb.OrderBook{
		Symbol:    orderBook.Symbol,
		Timestamp: orderBook.TimeStamp,
		Bids:      make([]*pb.PriceLevel, 0, len(orderBook.Bids.Price)),
		Asks:      make([]*pb.PriceLevel, 0, len(orderBook.Asks.Price)),
	}

	// 转换买单
	for i := 0; i < len(orderBook.Bids.Price) && i < len(orderBook.Bids.Size); i++ {
		bid := &pb.PriceLevel{
			Price:    orderBook.Bids.Price[i],
			Quantity: orderBook.Bids.Size[i],
		}
		protocolOrderBook.Bids = append(protocolOrderBook.Bids, bid)
	}

	// 转换卖单
	for i := 0; i < len(orderBook.Asks.Price) && i < len(orderBook.Asks.Size); i++ {
		ask := &pb.PriceLevel{
			Price:    orderBook.Asks.Price[i],
			Quantity: orderBook.Asks.Size[i],
		}
		protocolOrderBook.Asks = append(protocolOrderBook.Asks, ask)
	}

	resp := &pb.FetchOrderBookResponse{
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

	return conn.SendResponse(pb.ActionType_FETCH_ORDERBOOK, msg.Id, respData)
}

// HandleHealth 处理健康检查请求
func (h *Handlers) HandleHealth(conn server.Connection, msg *pb.Message) error {
	health := h.dataHive.Health()

	// 简化健康检查响应，直接返回JSON字符串
	respData := fmt.Sprintf(`{"status":"%s","timestamp":%d}`, health.Status, health.Timestamp)

	return conn.SendResponse(pb.ActionType_SUBSCRIBE, msg.Id, []byte(respData))
}

// convertDataTypeToEventType 转换新的DataType枚举到EventType
func convertDataTypeToEventType(dataType pb.DataType) (EventType, error) {
	switch dataType {
	case pb.DataType_MINI_TICKER:
		return EventMiniTicker, nil
	case pb.DataType_BOOK_TICKER:
		return EventBookTicker, nil
	case pb.DataType_KLINE:
		return EventKline, nil
	case pb.DataType_TRADE:
		return EventTrade, nil
	case pb.DataType_ORDERBOOK:
		return EventOrderBook, nil
	case pb.DataType_MARK_PRICE:
		return EventMarkPrice, nil
	default:
		return "", fmt.Errorf("unsupported data type: %s", dataType.String())
	}
}

// convertToOptionsFromNewReq 从新的请求结构转换到选项
func convertToOptionsFromNewReq(req *pb.SubscribeRequest) Options {
	return Options{
		Interval: req.KlineInterval,
		Depth:    int(req.OrderbookDepth),
	}
}

// HandleFetchTrades 处理获取交易记录请求
func (h *Handlers) HandleFetchTrades(conn server.Connection, msg *pb.Message) error {
	ctx := context.Background() // 创建context用于日志

	var req pb.FetchTradesRequest
	if err := proto.Unmarshal(msg.Data, &req); err != nil {
		logger.Ctx(ctx).Error("解析获取交易记录请求失败", zap.Error(err))
		return conn.SendError(msg.Id, 400, "invalid request format")
	}

	// 实际调用DataHive获取交易记录
	trades, err := h.dataHive.FetchTrades(ctx, req.Exchange, req.MarketType, req.Symbol, int(req.Since), int(req.Limit))
	if err != nil {
		logger.Ctx(ctx).Error("获取交易记录失败",
			zap.String("exchange", req.Exchange),
			zap.String("symbol", req.Symbol),
			zap.Error(err))
		return conn.SendError(msg.Id, 500, fmt.Sprintf("failed to fetch trades: %v", err))
	}

	// 转换ccxt.Trade到pb.Trade
	protocolTrades := make([]*pb.Trade, 0, len(trades))
	for _, trade := range trades {
		protocolTrade := &pb.Trade{
			Id:        trade.ID,
			Symbol:    trade.Symbol,
			Exchange:  req.Exchange,
			Price:     trade.Price,
			Quantity:  trade.Amount,
			Side:      trade.Side,
			Timestamp: trade.Timestamp,
		}
		protocolTrades = append(protocolTrades, protocolTrade)
	}

	resp := &pb.FetchTradesResponse{
		Trades: protocolTrades,
	}

	respData, err := proto.Marshal(resp)
	if err != nil {
		return conn.SendError(msg.Id, 500, "failed to marshal response")
	}

	logger.Ctx(ctx).Info("返回交易记录",
		zap.String("exchange", req.Exchange),
		zap.String("symbol", req.Symbol),
		zap.Int("count", len(protocolTrades)))

	return conn.SendResponse(pb.ActionType_FETCH_TRADES, msg.Id, respData)
}

// filterMarkets 根据市场类型和质押物类型筛选市场
func filterMarkets(markets []*ccxt.Market, marketType, stackType string) []*ccxt.Market {
	if marketType == "" && stackType == "" {
		return markets // 如果没有筛选条件，返回所有市场
	}

	var filtered []*ccxt.Market

	for _, market := range markets {
		// 根据市场类型筛选
		if marketType != "" {
			switch marketType {
			case "spot":
				if !market.Spot {
					continue
				}
			case "future", "futures":
				// 期货包括传统期货和永续合约
				if !market.Future && !market.Swap {
					continue
				}
			case "swap":
				if !market.Swap {
					continue
				}
			case "margin":
				if !market.Margin {
					continue
				}
			default:
				// 如果指定了未知的市场类型，检查Type字段
				if market.Type != marketType {
					continue
				}
			}
		}

		// 根据质押物类型筛选
		if stackType != "" {
			// 检查symbol中是否包含指定的质押物类型
			// 例如: stackType="BTC" 可以匹配 "BTC/USDT", "ETH/BTC" 等
			symbolContainsStackType := false

			// 检查基础货币或计价货币是否匹配
			if market.Base == stackType || market.Quote == stackType {
				symbolContainsStackType = true
			}

			// 检查完整symbol是否包含stackType
			if !symbolContainsStackType && strings.Contains(market.Symbol, stackType) {
				symbolContainsStackType = true
			}

			if !symbolContainsStackType {
				continue
			}
		}

		// 通过所有筛选条件的市场
		filtered = append(filtered, market)
	}

	return filtered
}

// minInt 返回两个整数中的较小值
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// convertCCXTMarketToProtocol 将 ccxt.Market 转换为 pb.Market
func convertCCXTMarketToProtocol(market *ccxt.Market) *pb.Market {
	// 转换精度信息
	precision := &pb.MarketPrecision{
		Amount: market.Precision.Amount,
		Price:  market.Precision.Price,
		Cost:   market.Precision.Cost,
	}

	// 转换限制信息
	limits := &pb.MarketLimits{
		Leverage: &pb.LimitRange{
			Min: market.Limits.Leverage.Min,
			Max: market.Limits.Leverage.Max,
		},
		Amount: &pb.LimitRange{
			Min: market.Limits.Amount.Min,
			Max: market.Limits.Amount.Max,
		},
		Price: &pb.LimitRange{
			Min: market.Limits.Price.Min,
			Max: market.Limits.Price.Max,
		},
		Cost: &pb.LimitRange{
			Min: market.Limits.Cost.Min,
			Max: market.Limits.Cost.Max,
		},
	}

	// 将 Info 字段序列化为 JSON 字符串
	var infoJSON string
	if market.Info != nil {
		if infoBytes, err := json.Marshal(market.Info); err == nil {
			infoJSON = string(infoBytes)
		}
	}

	return &pb.Market{
		Id:             market.ID,
		Symbol:         market.Symbol,
		Base:           market.Base,
		Quote:          market.Quote,
		Settle:         market.Settle,
		Type:           market.Type,
		Spot:           market.Spot,
		Margin:         market.Margin,
		Swap:           market.Swap,
		Future:         market.Future,
		Option:         market.Option,
		Active:         market.Active,
		Contract:       market.Contract,
		Linear:         market.Linear,
		Inverse:        market.Inverse,
		MakerFee:       market.Maker,
		TakerFee:       market.Taker,
		ContractSize:   market.ContractSize,
		Expiry:         market.Expiry,
		ExpiryDatetime: market.ExpiryDatetime,
		Strike:         market.Strike,
		OptionType:     market.OptionType,
		Precision:      precision,
		Limits:         limits,
		Info:           infoJSON,
	}
}
