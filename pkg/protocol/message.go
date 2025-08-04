package protocol

import (
	"datahive/pkg/protocol/pb"
)

// =====================================================================================
// 基础类型别名
// =====================================================================================

type Message = pb.Message
type MessageType = pb.MessageType
type ActionType = pb.ActionType
type Error = pb.Error

// =====================================================================================
// 数据结构类型别名
// =====================================================================================

type Market = pb.Market
type Ticker = pb.Ticker
type Kline = pb.Kline
type Trade = pb.Trade
type OrderBook = pb.OrderBook
type PriceLevel = pb.PriceLevel

// =====================================================================================
// 订阅管理类型别名
// =====================================================================================

type SubscribeRequest = pb.SubscribeRequest
type SubscribeResponse = pb.SubscribeResponse
type UnsubscribeRequest = pb.UnsubscribeRequest
type UnsubscribeResponse = pb.UnsubscribeResponse

// =====================================================================================
// 数据获取类型别名
// =====================================================================================

type FetchMarketsRequest = pb.FetchMarketsRequest
type FetchMarketsResponse = pb.FetchMarketsResponse
type FetchTickerRequest = pb.FetchTickerRequest
type FetchTickerResponse = pb.FetchTickerResponse
type FetchKlinesRequest = pb.FetchKlinesRequest
type FetchKlinesResponse = pb.FetchKlinesResponse
type FetchTradesRequest = pb.FetchTradesRequest
type FetchTradesResponse = pb.FetchTradesResponse
type FetchOrderBookRequest = pb.FetchOrderBookRequest
type FetchOrderBookResponse = pb.FetchOrderBookResponse

// =====================================================================================
// 数据推送类型别名
// =====================================================================================

type TickerUpdate = pb.TickerUpdate
type KlineUpdate = pb.KlineUpdate
type TradeUpdate = pb.TradeUpdate
type OrderBookUpdate = pb.OrderBookUpdate

// =====================================================================================
// 常量定义
// =====================================================================================

// MessageType 常量
const (
	TypeRequest      = pb.MessageType_REQUEST
	TypeResponse     = pb.MessageType_RESPONSE
	TypeNotification = pb.MessageType_NOTIFICATION
	TypeError        = pb.MessageType_ERROR
)

// ActionType 常量
const (
	// 订阅管理
	ActionSubscribe   = pb.ActionType_SUBSCRIBE
	ActionUnsubscribe = pb.ActionType_UNSUBSCRIBE

	// 数据获取
	ActionFetchMarkets   = pb.ActionType_FETCH_MARKETS
	ActionFetchTicker    = pb.ActionType_FETCH_TICKER
	ActionFetchKlines    = pb.ActionType_FETCH_KLINES
	ActionFetchTrades    = pb.ActionType_FETCH_TRADES
	ActionFetchOrderBook = pb.ActionType_FETCH_ORDERBOOK

	// 数据推送
	ActionMarketUpdate    = pb.ActionType_MARKET_UPDATE
	ActionTickerUpdate    = pb.ActionType_TICKER_UPDATE
	ActionKlineUpdate     = pb.ActionType_KLINE_UPDATE
	ActionTradeUpdate     = pb.ActionType_TRADE_UPDATE
	ActionOrderBookUpdate = pb.ActionType_ORDERBOOK_UPDATE
)

// =====================================================================================
// 向后兼容的常量 (临时保留)
// =====================================================================================

const (
	// 旧的Action常量 (映射到新的常量)
	ActionStatus    = ActionTickerUpdate // 临时映射
	ActionHeartbeat = ActionTickerUpdate // 临时映射
	ActionPing      = ActionTickerUpdate // 临时映射
	ActionPong      = ActionTickerUpdate // 临时映射

	// 旧的数据推送常量
	ActionTickersUpdate = ActionTickerUpdate // 重复使用
	ActionOHLCVUpdate   = ActionKlineUpdate  // 映射到新的
	ActionTradesUpdate  = ActionTradeUpdate  // 映射到新的
)
