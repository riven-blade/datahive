package storage

import (
	"time"

	"datahive/pkg/ccxt"
	"datahive/pkg/protocol/pb"
)

// TypeConverter 类型转换器，处理protocol和ccxt类型之间的转换
type TypeConverter struct{}

// NewTypeConverter 创建类型转换器
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// CCXTTickerToProtocol 将CCXT Ticker转换为Protocol Ticker
func (tc *TypeConverter) CCXTTickerToProtocol(exchange string, ticker *ccxt.Ticker) *pb.Ticker {
	if ticker == nil {
		return nil
	}

	return &pb.Ticker{
		Symbol:        ticker.Symbol,
		Timestamp:     ticker.TimeStamp,
		Last:          ticker.Last,
		Bid:           ticker.Bid,
		Ask:           ticker.Ask,
		High:          ticker.High,
		Low:           ticker.Low,
		Open:          ticker.Open,
		Close:         ticker.Close,
		Volume:        ticker.BaseVolume, // 使用基础货币成交量
		Change:        ticker.Change,
		ChangePercent: ticker.Percentage,
	}
}

// ProtocolTickerToCCXT 将Protocol Ticker转换为CCXT Ticker
func (tc *TypeConverter) ProtocolTickerToCCXT(ticker *pb.Ticker) *ccxt.Ticker {
	if ticker == nil {
		return nil
	}

	return &ccxt.Ticker{
		Symbol:     ticker.Symbol,
		TimeStamp:  ticker.Timestamp,
		Last:       ticker.Last,
		Bid:        ticker.Bid,
		Ask:        ticker.Ask,
		High:       ticker.High,
		Low:        ticker.Low,
		Open:       ticker.Open,
		Close:      ticker.Close,
		BaseVolume: ticker.Volume,
		Change:     ticker.Change,
		Percentage: ticker.ChangePercent,
		Datetime:   time.UnixMilli(ticker.Timestamp).UTC().Format(time.RFC3339),
	}
}

// CCXTOHLCVToProtocolKline 将CCXT OHLCV转换为Protocol Kline
func (tc *TypeConverter) CCXTOHLCVToProtocolKline(exchange, symbol, timeframe string, ohlcv *ccxt.OHLCV) *pb.Kline {
	if ohlcv == nil {
		return nil
	}

	return &pb.Kline{
		Symbol:    symbol,
		Exchange:  exchange,
		Timeframe: timeframe,
		OpenTime:  ohlcv.Timestamp,
		Open:      ohlcv.Open,
		High:      ohlcv.High,
		Low:       ohlcv.Low,
		Close:     ohlcv.Close,
		Volume:    ohlcv.Volume,
		Closed:    true, // CCXT的OHLCV数据默认认为是已完成的
	}
}

// ProtocolKlineToCCXTOHLCV 将Protocol Kline转换为CCXT OHLCV
func (tc *TypeConverter) ProtocolKlineToCCXTOHLCV(kline *pb.Kline) *ccxt.OHLCV {
	if kline == nil {
		return nil
	}

	return &ccxt.OHLCV{
		Timestamp: kline.OpenTime,
		Open:      kline.Open,
		High:      kline.High,
		Low:       kline.Low,
		Close:     kline.Close,
		Volume:    kline.Volume,
	}
}

// CCXTTradeToProtocol 将CCXT Trade转换为Protocol Trade
func (tc *TypeConverter) CCXTTradeToProtocol(exchange string, trade *ccxt.Trade) *pb.Trade {
	if trade == nil {
		return nil
	}

	return &pb.Trade{
		Id:        trade.ID,
		Symbol:    trade.Symbol,
		Exchange:  exchange,
		Price:     trade.Price,
		Quantity:  trade.Amount,
		Side:      trade.Side,
		Timestamp: trade.Timestamp,
	}
}

// ProtocolTradeToCCXT 将Protocol Trade转换为CCXT Trade
func (tc *TypeConverter) ProtocolTradeToCCXT(trade *pb.Trade) *ccxt.Trade {
	if trade == nil {
		return nil
	}

	return &ccxt.Trade{
		ID:        trade.Id,
		Symbol:    trade.Symbol,
		Amount:    trade.Quantity,
		Price:     trade.Price,
		Side:      trade.Side,
		Timestamp: trade.Timestamp,
		Datetime:  time.UnixMilli(trade.Timestamp).UTC().Format(time.RFC3339),
	}
}

// CCXTOrderBookToProtocol 将CCXT OrderBook转换为Protocol OrderBook
func (tc *TypeConverter) CCXTOrderBookToProtocol(exchange string, orderBook *ccxt.OrderBook) *pb.OrderBook {
	if orderBook == nil {
		return nil
	}

	// 转换买单
	bids := make([]*pb.PriceLevel, 0, len(orderBook.Bids.Price))
	for i := 0; i < len(orderBook.Bids.Price) && i < len(orderBook.Bids.Size); i++ {
		bids = append(bids, &pb.PriceLevel{
			Price:    orderBook.Bids.Price[i],
			Quantity: orderBook.Bids.Size[i],
		})
	}

	// 转换卖单
	asks := make([]*pb.PriceLevel, 0, len(orderBook.Asks.Price))
	for i := 0; i < len(orderBook.Asks.Price) && i < len(orderBook.Asks.Size); i++ {
		asks = append(asks, &pb.PriceLevel{
			Price:    orderBook.Asks.Price[i],
			Quantity: orderBook.Asks.Size[i],
		})
	}

	return &pb.OrderBook{
		Symbol:    orderBook.Symbol,
		Timestamp: orderBook.TimeStamp,
		Bids:      bids,
		Asks:      asks,
	}
}

// ProtocolOrderBookToCCXT 将Protocol OrderBook转换为CCXT OrderBook
func (tc *TypeConverter) ProtocolOrderBookToCCXT(orderBook *pb.OrderBook) *ccxt.OrderBook {
	if orderBook == nil {
		return nil
	}

	// 转换买单
	bidPrices := make([]float64, 0, len(orderBook.Bids))
	bidSizes := make([]float64, 0, len(orderBook.Bids))
	for _, bid := range orderBook.Bids {
		bidPrices = append(bidPrices, bid.Price)
		bidSizes = append(bidSizes, bid.Quantity)
	}

	// 转换卖单
	askPrices := make([]float64, 0, len(orderBook.Asks))
	askSizes := make([]float64, 0, len(orderBook.Asks))
	for _, ask := range orderBook.Asks {
		askPrices = append(askPrices, ask.Price)
		askSizes = append(askSizes, ask.Quantity)
	}

	return &ccxt.OrderBook{
		Symbol:    orderBook.Symbol,
		Bids:      ccxt.OrderBookSide{Price: bidPrices, Size: bidSizes},
		Asks:      ccxt.OrderBookSide{Price: askPrices, Size: askSizes},
		TimeStamp: orderBook.Timestamp,
		Datetime:  time.UnixMilli(orderBook.Timestamp).UTC().Format(time.RFC3339),
	}
}

// BatchConvertCCXTTickers 批量转换CCXT Ticker
func (tc *TypeConverter) BatchConvertCCXTTickers(exchange string, tickers []*ccxt.Ticker) []*pb.Ticker {
	if len(tickers) == 0 {
		return nil
	}

	result := make([]*pb.Ticker, 0, len(tickers))
	for _, ticker := range tickers {
		if converted := tc.CCXTTickerToProtocol(exchange, ticker); converted != nil {
			result = append(result, converted)
		}
	}
	return result
}

// BatchConvertCCXTOHLCVs 批量转换CCXT OHLCV
func (tc *TypeConverter) BatchConvertCCXTOHLCVs(exchange, symbol, timeframe string, ohlcvs []*ccxt.OHLCV) []*pb.Kline {
	if len(ohlcvs) == 0 {
		return nil
	}

	result := make([]*pb.Kline, 0, len(ohlcvs))
	for _, ohlcv := range ohlcvs {
		if converted := tc.CCXTOHLCVToProtocolKline(exchange, symbol, timeframe, ohlcv); converted != nil {
			result = append(result, converted)
		}
	}
	return result
}

// BatchConvertCCXTTrades 批量转换CCXT Trade
func (tc *TypeConverter) BatchConvertCCXTTrades(exchange string, trades []*ccxt.Trade) []*pb.Trade {
	if len(trades) == 0 {
		return nil
	}

	result := make([]*pb.Trade, 0, len(trades))
	for _, trade := range trades {
		if converted := tc.CCXTTradeToProtocol(exchange, trade); converted != nil {
			result = append(result, converted)
		}
	}
	return result
}

// ValidateTickerData 验证Ticker数据的有效性
func (tc *TypeConverter) ValidateTickerData(ticker *pb.Ticker) error {
	if ticker == nil {
		return ErrInvalidData("ticker is nil")
	}
	if ticker.Symbol == "" {
		return ErrInvalidData("ticker symbol is empty")
	}
	if ticker.Timestamp <= 0 {
		return ErrInvalidData("ticker timestamp is invalid")
	}
	if ticker.Last < 0 || ticker.Bid < 0 || ticker.Ask < 0 {
		return ErrInvalidData("ticker contains negative prices")
	}
	if ticker.Volume < 0 {
		return ErrInvalidData("ticker volume is negative")
	}
	return nil
}

// ValidateKlineData 验证Kline数据的有效性
func (tc *TypeConverter) ValidateKlineData(kline *pb.Kline) error {
	if kline == nil {
		return ErrInvalidData("kline is nil")
	}
	if kline.Symbol == "" {
		return ErrInvalidData("kline symbol is empty")
	}
	if kline.Exchange == "" {
		return ErrInvalidData("kline exchange is empty")
	}
	if kline.Timeframe == "" {
		return ErrInvalidData("kline timeframe is empty")
	}
	if kline.OpenTime <= 0 {
		return ErrInvalidData("kline open_time is invalid")
	}
	if kline.Open < 0 || kline.High < 0 || kline.Low < 0 || kline.Close < 0 {
		return ErrInvalidData("kline contains negative prices")
	}
	if kline.High < kline.Low {
		return ErrInvalidData("kline high price is less than low price")
	}
	if kline.Volume < 0 {
		return ErrInvalidData("kline volume is negative")
	}
	return nil
}

// ValidateTradeData 验证Trade数据的有效性
func (tc *TypeConverter) ValidateTradeData(trade *pb.Trade) error {
	if trade == nil {
		return ErrInvalidData("trade is nil")
	}
	if trade.Id == "" {
		return ErrInvalidData("trade id is empty")
	}
	if trade.Symbol == "" {
		return ErrInvalidData("trade symbol is empty")
	}
	if trade.Exchange == "" {
		return ErrInvalidData("trade exchange is empty")
	}
	if trade.Price <= 0 {
		return ErrInvalidData("trade price is invalid")
	}
	if trade.Quantity <= 0 {
		return ErrInvalidData("trade quantity is invalid")
	}
	if trade.Side != "buy" && trade.Side != "sell" {
		return ErrInvalidData("trade side must be 'buy' or 'sell'")
	}
	if trade.Timestamp <= 0 {
		return ErrInvalidData("trade timestamp is invalid")
	}
	return nil
}
