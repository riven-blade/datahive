package storage

import (
	"context"
	"datahive/pkg/protocol"
	"time"
)

// TimeSeriesStorage 时序数据存储接口
type TimeSeriesStorage interface {
	Connect(ctx context.Context) error
	Close() error
	Ping(ctx context.Context) error
	IsHealthy() bool

	SaveKlines(ctx context.Context, exchange string, klines []*protocol.Kline) error
	QueryKlines(ctx context.Context, exchange, symbol, timeframe string, start int64, limit int) ([]*protocol.Kline, error)

	SaveTrades(ctx context.Context, exchange string, trades []*protocol.Trade) error
	QueryTrades(ctx context.Context, exchange, symbol string, start int64, limit int) ([]*protocol.Trade, error)

	SaveTickers(ctx context.Context, exchange string, prices []*protocol.Ticker) error
	QueryTickers(ctx context.Context, exchange, symbol string, start int64, limit int) ([]*protocol.Ticker, error)

	DeleteExpiredData(ctx context.Context, beforeTime int64) error
}

// KVStorage 键值存储接口
type KVStorage interface {
	Connect(ctx context.Context) error
	Close() error
	Ping(ctx context.Context) error
	IsHealthy() bool

	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, keys ...string) error
	Exists(ctx context.Context, key string) (bool, error)

	MSet(ctx context.Context, pairs map[string][]byte, expiration time.Duration) error
	MGet(ctx context.Context, keys ...string) (map[string][]byte, error)

	HSet(ctx context.Context, key, field string, value []byte) error
	HGet(ctx context.Context, key, field string) ([]byte, error)
	HGetAll(ctx context.Context, key string) (map[string][]byte, error)
	HDel(ctx context.Context, key string, fields ...string) error

	LPush(ctx context.Context, key string, values ...[]byte) error
	RPop(ctx context.Context, key string) ([]byte, error)
	LLen(ctx context.Context, key string) (int64, error)

	SAdd(ctx context.Context, key string, members ...[]byte) error
	SMembers(ctx context.Context, key string) ([][]byte, error)
	SIsMember(ctx context.Context, key string, member []byte) (bool, error)

	Expire(ctx context.Context, key string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)

	FlushDB(ctx context.Context) error

	SetTicket(ctx context.Context, exchange string, ticket *protocol.Ticker, expiration time.Duration) error
	GetTicket(ctx context.Context, exchange string, symbol string) (*protocol.Ticker, error)
}
