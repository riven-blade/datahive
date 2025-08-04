package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/riven-blade/datahive/config"
	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol/pb"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// RedisStorage Redis 键值存储实现
type RedisStorage struct {
	config        *config.RedisConfig
	client        *redis.Client
	typeConverter *TypeConverter
	isOpen        int32
	mu            sync.RWMutex

	// 健康检查
	healthStatus    int32
	lastHealthCheck time.Time

	// 统计信息
	stats *KVStorageStats
}

// KVStorageStats Redis存储统计信息
type KVStorageStats struct {
	mu sync.RWMutex

	// 连接统计
	ConnectionCount    int64
	ReconnectionCount  int64
	LastConnectionTime time.Time

	// 操作统计
	TotalOperations  int64
	SuccessfulOps    int64
	FailedOps        int64
	LastError        time.Time
	LastErrorMessage string

	// 数据统计
	GetOperations    int64
	SetOperations    int64
	DeleteOperations int64
	CacheHits        int64
	CacheMisses      int64

	// 性能统计
	AverageLatencyMs float64
	MaxLatencyMs     float64
}

// NewRedisStorage 创建并连接 Redis 存储实例
func NewRedisStorage(ctx context.Context, conf *config.RedisConfig) (*RedisStorage, error) {
	if conf == nil {
		conf = config.NewRedisConfig()
	}

	storage := &RedisStorage{
		config:        conf,
		typeConverter: NewTypeConverter(),
		stats:         &KVStorageStats{},
	}

	// 立即建立连接
	if err := storage.Connect(ctx); err != nil {
		return nil, ErrConnectionError("failed to initialize Redis storage", err)
	}

	// 启动优雅退出监听
	go func() {
		<-ctx.Done()

		logger.Ctx(context.Background()).Info("收到退出信号，开始优雅关闭Redis存储...")
		if err := storage.Close(); err != nil {
			logger.Ctx(context.Background()).Error("Redis存储关闭失败", zap.Error(err))
		} else {
			logger.Ctx(context.Background()).Info("Redis存储已优雅关闭")
		}
	}()

	return storage, nil
}

// Connect 连接到 Redis
func (r *RedisStorage) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if atomic.LoadInt32(&r.isOpen) == 1 {
		return nil
	}

	r.recordConnectionAttempt()

	// 创建Redis客户端
	r.client = redis.NewClient(&redis.Options{
		Addr:            fmt.Sprintf("%s:%d", r.config.Host, r.config.Port),
		Password:        r.config.Password,
		DB:              r.config.Database,
		DialTimeout:     r.config.ConnectionTimeout,
		ReadTimeout:     r.config.QueryTimeout,
		WriteTimeout:    r.config.QueryTimeout,
		MaxRetries:      3,
		MaxRetryBackoff: time.Second,
		PoolSize:        r.config.MaxConnections,
		MinIdleConns:    10,
		IdleTimeout:     5 * time.Minute,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(ctx, r.config.ConnectionTimeout)
	defer cancel()

	if err := r.client.Ping(ctx).Err(); err != nil {
		r.recordFailedOperation("ping failed")
		return ErrConnectionError("failed to ping Redis", err)
	}

	atomic.StoreInt32(&r.isOpen, 1)
	atomic.StoreInt32(&r.healthStatus, 1)

	logger.Ctx(ctx).Info("Redis连接成功",
		zap.String("host", r.config.Host),
		zap.Int("port", r.config.Port),
		zap.Int("database", r.config.Database))

	return nil
}

// Close 关闭连接
func (r *RedisStorage) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if atomic.LoadInt32(&r.isOpen) == 0 {
		return nil
	}

	if r.client != nil {
		if err := r.client.Close(); err != nil {
			return ErrConnectionError("failed to close Redis connection", err)
		}
		r.client = nil
	}

	atomic.StoreInt32(&r.isOpen, 0)
	atomic.StoreInt32(&r.healthStatus, 0)

	logger.Ctx(context.Background()).Info("Redis连接已关闭")
	return nil
}

// Ping 检查连接状态
func (r *RedisStorage) Ping(ctx context.Context) error {
	if atomic.LoadInt32(&r.isOpen) == 0 {
		return ErrConnectionClosed
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	if err := r.client.Ping(ctx).Err(); err != nil {
		r.recordFailedOperation("ping failed")
		return ErrConnectionError("ping failed", err)
	}

	return nil
}

// IsHealthy 检查存储健康状态
func (r *RedisStorage) IsHealthy() bool {
	return atomic.LoadInt32(&r.isOpen) == 1 && atomic.LoadInt32(&r.healthStatus) == 1
}

// Set 设置键值对
func (r *RedisStorage) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if key == "" {
		return ErrInvalidData("key cannot be empty")
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	if err := r.client.Set(ctx, key, value, expiration).Err(); err != nil {
		r.recordFailedOperation("set operation failed")
		return ErrQueryError("failed to set value", err)
	}

	r.recordSuccessfulOperation()
	r.updateSetStats(time.Since(start))

	return nil
}

// Get 获取值
func (r *RedisStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if key == "" {
		return nil, ErrInvalidData("key cannot be empty")
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			r.updateGetStats(time.Since(start), false)
			return nil, ErrNotFoundError("key not found")
		}
		r.recordFailedOperation("get operation failed")
		return nil, ErrQueryError("failed to get value", err)
	}

	r.recordSuccessfulOperation()
	r.updateGetStats(time.Since(start), true)

	return []byte(result), nil
}

// Delete 删除键
func (r *RedisStorage) Delete(ctx context.Context, keys ...string) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if len(keys) == 0 {
		return nil
	}

	for _, key := range keys {
		if key == "" {
			return ErrInvalidData("key cannot be empty")
		}
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		r.recordFailedOperation("delete operation failed")
		return ErrQueryError("failed to delete keys", err)
	}

	r.recordSuccessfulOperation()
	r.updateDeleteStats(time.Since(start))

	return nil
}

// Exists 检查键是否存在
func (r *RedisStorage) Exists(ctx context.Context, key string) (bool, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return false, ErrStorageNotHealthy
	}

	if key == "" {
		return false, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		r.recordFailedOperation("exists operation failed")
		return false, ErrQueryError("failed to check key existence", err)
	}

	r.recordSuccessfulOperation()
	return result > 0, nil
}

// MSet 批量设置键值对
func (r *RedisStorage) MSet(ctx context.Context, pairs map[string][]byte, expiration time.Duration) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if len(pairs) == 0 {
		return nil
	}

	// 验证键
	for key := range pairs {
		if key == "" {
			return ErrInvalidData("key cannot be empty")
		}
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	// 准备数据
	values := make([]interface{}, 0, len(pairs)*2)
	for key, value := range pairs {
		values = append(values, key, value)
	}

	// 使用事务确保原子性
	pipe := r.client.TxPipeline()
	pipe.MSet(ctx, values...)

	// 如果有过期时间，为每个键设置过期时间
	if expiration > 0 {
		for key := range pairs {
			pipe.Expire(ctx, key, expiration)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		r.recordFailedOperation("mset operation failed")
		return ErrQueryError("failed to batch set values", err)
	}

	r.recordSuccessfulOperation()
	r.updateSetStats(time.Since(start))

	return nil
}

// MGet 批量获取值
func (r *RedisStorage) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}

	// 验证键
	for _, key := range keys {
		if key == "" {
			return nil, ErrInvalidData("key cannot be empty")
		}
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	results, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		r.recordFailedOperation("mget operation failed")
		return nil, ErrQueryError("failed to batch get values", err)
	}

	response := make(map[string][]byte)
	hits := 0
	for i, result := range results {
		if result != nil {
			if str, ok := result.(string); ok {
				response[keys[i]] = []byte(str)
				hits++
			}
		}
	}

	r.recordSuccessfulOperation()
	r.updateGetStats(time.Since(start), hits > 0)

	return response, nil
}

// HSet 设置哈希字段
func (r *RedisStorage) HSet(ctx context.Context, key, field string, value []byte) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if key == "" || field == "" {
		return ErrInvalidData("key and field cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	if err := r.client.HSet(ctx, key, field, value).Err(); err != nil {
		r.recordFailedOperation("hset operation failed")
		return ErrQueryError("failed to set hash field", err)
	}

	r.recordSuccessfulOperation()
	return nil
}

// HGet 获取哈希字段值
func (r *RedisStorage) HGet(ctx context.Context, key, field string) ([]byte, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if key == "" || field == "" {
		return nil, ErrInvalidData("key and field cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.HGet(ctx, key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNotFoundError("hash field not found")
		}
		r.recordFailedOperation("hget operation failed")
		return nil, ErrQueryError("failed to get hash field", err)
	}

	r.recordSuccessfulOperation()
	return []byte(result), nil
}

// HGetAll 获取所有哈希字段
func (r *RedisStorage) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if key == "" {
		return nil, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	results, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		r.recordFailedOperation("hgetall operation failed")
		return nil, ErrQueryError("failed to get all hash fields", err)
	}

	response := make(map[string][]byte)
	for field, value := range results {
		response[field] = []byte(value)
	}

	r.recordSuccessfulOperation()
	return response, nil
}

// HDel 删除哈希字段
func (r *RedisStorage) HDel(ctx context.Context, key string, fields ...string) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if key == "" {
		return ErrInvalidData("key cannot be empty")
	}

	if len(fields) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	if err := r.client.HDel(ctx, key, fields...).Err(); err != nil {
		r.recordFailedOperation("hdel operation failed")
		return ErrQueryError("failed to delete hash fields", err)
	}

	r.recordSuccessfulOperation()
	return nil
}

// LPush 左推入列表
func (r *RedisStorage) LPush(ctx context.Context, key string, values ...[]byte) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if key == "" {
		return ErrInvalidData("key cannot be empty")
	}

	if len(values) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	// 转换为interface{}数组
	args := make([]interface{}, len(values))
	for i, v := range values {
		args[i] = v
	}

	if err := r.client.LPush(ctx, key, args...).Err(); err != nil {
		r.recordFailedOperation("lpush operation failed")
		return ErrQueryError("failed to push values to list", err)
	}

	r.recordSuccessfulOperation()
	return nil
}

// RPop 右弹出列表
func (r *RedisStorage) RPop(ctx context.Context, key string) ([]byte, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if key == "" {
		return nil, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.RPop(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNotFoundError("list is empty")
		}
		r.recordFailedOperation("rpop operation failed")
		return nil, ErrQueryError("failed to pop value from list", err)
	}

	r.recordSuccessfulOperation()
	return []byte(result), nil
}

// LLen 获取列表长度
func (r *RedisStorage) LLen(ctx context.Context, key string) (int64, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return 0, ErrStorageNotHealthy
	}

	if key == "" {
		return 0, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.LLen(ctx, key).Result()
	if err != nil {
		r.recordFailedOperation("llen operation failed")
		return 0, ErrQueryError("failed to get list length", err)
	}

	r.recordSuccessfulOperation()
	return result, nil
}

// SAdd 添加集合成员
func (r *RedisStorage) SAdd(ctx context.Context, key string, members ...[]byte) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if key == "" {
		return ErrInvalidData("key cannot be empty")
	}

	if len(members) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	// 转换为interface{}数组
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}

	if err := r.client.SAdd(ctx, key, args...).Err(); err != nil {
		r.recordFailedOperation("sadd operation failed")
		return ErrQueryError("failed to add members to set", err)
	}

	r.recordSuccessfulOperation()
	return nil
}

// SMembers 获取集合所有成员
func (r *RedisStorage) SMembers(ctx context.Context, key string) ([][]byte, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return nil, ErrStorageNotHealthy
	}

	if key == "" {
		return nil, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	results, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		r.recordFailedOperation("smembers operation failed")
		return nil, ErrQueryError("failed to get set members", err)
	}

	response := make([][]byte, len(results))
	for i, member := range results {
		response[i] = []byte(member)
	}

	r.recordSuccessfulOperation()
	return response, nil
}

// SIsMember 检查是否为集合成员
func (r *RedisStorage) SIsMember(ctx context.Context, key string, member []byte) (bool, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return false, ErrStorageNotHealthy
	}

	if key == "" {
		return false, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.SIsMember(ctx, key, member).Result()
	if err != nil {
		r.recordFailedOperation("sismember operation failed")
		return false, ErrQueryError("failed to check set membership", err)
	}

	r.recordSuccessfulOperation()
	return result, nil
}

// Expire 设置过期时间
func (r *RedisStorage) Expire(ctx context.Context, key string, expiration time.Duration) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	if key == "" {
		return ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	if err := r.client.Expire(ctx, key, expiration).Err(); err != nil {
		r.recordFailedOperation("expire operation failed")
		return ErrQueryError("failed to set expiration", err)
	}

	r.recordSuccessfulOperation()
	return nil
}

// TTL 获取剩余过期时间
func (r *RedisStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return 0, ErrStorageNotHealthy
	}

	if key == "" {
		return 0, ErrInvalidData("key cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout)
	defer cancel()

	result, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		r.recordFailedOperation("ttl operation failed")
		return 0, ErrQueryError("failed to get TTL", err)
	}

	r.recordSuccessfulOperation()
	return result, nil
}

// FlushDB 清空数据库
func (r *RedisStorage) FlushDB(ctx context.Context) error {
	if !r.IsHealthy() {
		r.recordFailedOperation("storage not healthy")
		return ErrStorageNotHealthy
	}

	ctx, cancel := context.WithTimeout(ctx, r.config.QueryTimeout*2)
	defer cancel()

	if err := r.client.FlushDB(ctx).Err(); err != nil {
		r.recordFailedOperation("flushdb operation failed")
		return ErrQueryError("failed to flush database", err)
	}

	r.recordSuccessfulOperation()
	logger.Ctx(ctx).Warn("Redis数据库已清空")
	return nil
}

// SetTicket 设置价格缓存
func (r *RedisStorage) SetTicket(ctx context.Context, exchange string, ticker *pb.Ticker, expiration time.Duration) error {
	if ticker == nil {
		return ErrInvalidData("ticker cannot be nil")
	}

	if err := r.typeConverter.ValidateTickerData(ticker); err != nil {
		return ErrValidationError(fmt.Sprintf("invalid ticker data: %v", err))
	}

	key := fmt.Sprintf("ticker:%s:%s", exchange, ticker.Symbol)
	data, err := json.Marshal(ticker)
	if err != nil {
		return ErrQueryError("failed to marshal ticker", err)
	}

	return r.Set(ctx, key, data, expiration)
}

// GetTicket 获取价格缓存
func (r *RedisStorage) GetTicket(ctx context.Context, exchange string, symbol string) (*pb.Ticker, error) {
	if exchange == "" || symbol == "" {
		return nil, ErrInvalidData("exchange and symbol cannot be empty")
	}

	key := fmt.Sprintf("ticker:%s:%s", exchange, symbol)
	data, err := r.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var ticker pb.Ticker
	if err := json.Unmarshal(data, &ticker); err != nil {
		return nil, ErrQueryError("failed to unmarshal ticker", err)
	}

	return &ticker, nil
}

// 内部统计方法

func (r *RedisStorage) recordConnectionAttempt() {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	r.stats.ConnectionCount++
	r.stats.LastConnectionTime = time.Now()
}

func (r *RedisStorage) recordSuccessfulOperation() {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	r.stats.TotalOperations++
	r.stats.SuccessfulOps++
}

func (r *RedisStorage) recordFailedOperation(message string) {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	r.stats.TotalOperations++
	r.stats.FailedOps++
	r.stats.LastError = time.Now()
	r.stats.LastErrorMessage = message
}

func (r *RedisStorage) updateGetStats(latency time.Duration, hit bool) {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	r.stats.GetOperations++
	if hit {
		r.stats.CacheHits++
	} else {
		r.stats.CacheMisses++
	}
	r.updateLatency(latency)
}

func (r *RedisStorage) updateSetStats(latency time.Duration) {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	r.stats.SetOperations++
	r.updateLatency(latency)
}

func (r *RedisStorage) updateDeleteStats(latency time.Duration) {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()
	r.stats.DeleteOperations++
	r.updateLatency(latency)
}

func (r *RedisStorage) updateLatency(latency time.Duration) {
	newLatencyMs := float64(latency.Milliseconds())
	if r.stats.AverageLatencyMs == 0 {
		r.stats.AverageLatencyMs = newLatencyMs
	} else {
		r.stats.AverageLatencyMs = (r.stats.AverageLatencyMs*0.9 + newLatencyMs*0.1)
	}

	if newLatencyMs > r.stats.MaxLatencyMs {
		r.stats.MaxLatencyMs = newLatencyMs
	}
}

// GetRedisStats 获取Redis统计信息
func (r *RedisStorage) GetRedisStats() *KVStorageStats {
	r.stats.mu.RLock()
	defer r.stats.mu.RUnlock()

	statsCopy := *r.stats
	return &statsCopy
}
