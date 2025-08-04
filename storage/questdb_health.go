package storage

import (
	"context"
	"github.com/riven-blade/datahive/pkg/logger"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// start 启动健康检查器
func (hc *HealthChecker) start() error {
	logger.Ctx(context.Background()).Info("启动健康检查器",
		zap.Duration("checkInterval", hc.checkInterval),
		zap.Duration("timeout", hc.timeout),
		zap.Int("maxRetries", hc.maxRetries))

	hc.ticker = time.NewTicker(hc.checkInterval)

	hc.wg.Add(1)
	go func() {
		defer hc.wg.Done()
		defer func() {
			if hc.ticker != nil {
				hc.ticker.Stop()
			}
		}()

		for {
			select {
			case <-hc.ticker.C:
				hc.performHealthCheck()
			case <-hc.stopChan:
				return
			}
		}
	}()

	return nil
}

// stop 停止健康检查器
func (hc *HealthChecker) stop() {
	if hc.ticker != nil {
		hc.ticker.Stop()
		hc.ticker = nil
	}

	select {
	case <-hc.stopChan:
		// 已经关闭
	default:
		close(hc.stopChan)
	}

	hc.wg.Wait()
}

// performHealthCheck 执行健康检查
func (hc *HealthChecker) performHealthCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), hc.timeout)
	defer cancel()

	healthy := atomic.LoadInt32(&hc.storage.isOpen) == 1
	if healthy {
		// 检查数据库连接
		if err := hc.storage.db.PingContext(ctx); err != nil {
			healthy = false
			logger.Ctx(ctx).Warn("数据库ping失败", zap.Error(err))
		}
	}

	previousHealth := atomic.LoadInt32(&hc.storage.healthStatus)

	if healthy {
		atomic.StoreInt32(&hc.storage.healthStatus, 1)
	} else {
		atomic.StoreInt32(&hc.storage.healthStatus, 0)
	}

	hc.storage.lastHealthCheck = time.Now()

	// 状态变化时记录日志和处理重连
	if (healthy && previousHealth == 0) || (!healthy && previousHealth == 1) {
		if healthy {
			logger.Ctx(ctx).Info("存储健康状态恢复")
			hc.storage.recordSuccessfulOperation()
		} else {
			logger.Ctx(ctx).Warn("存储健康检查失败")
			hc.storage.recordFailedOperation("health check failed")

			// 尝试重连
			go hc.attemptReconnection()
		}
	}
}

// attemptReconnection 尝试重连
func (hc *HealthChecker) attemptReconnection() {
	if atomic.LoadInt32(&hc.storage.isOpen) == 0 {
		return
	}

	logger.Ctx(context.Background()).Info("尝试重新连接存储")

	for i := 0; i < hc.maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		// 尝试重新建立连接
		if err := hc.storage.connectDatabase(ctx); err != nil {
			cancel()
			hc.storage.recordFailedOperation("reconnection failed")
			logger.Ctx(context.Background()).Error("重连失败",
				zap.Error(err),
				zap.Int("attempt", i+1),
				zap.Int("maxRetries", hc.maxRetries))

			if i < hc.maxRetries-1 {
				time.Sleep(hc.retryDelay * time.Duration(i+1))
			}
			continue
		}

		cancel()

		// 重连成功
		atomic.StoreInt32(&hc.storage.healthStatus, 1)
		hc.storage.recordReconnection()

		// 重启批量处理器
		if err := hc.storage.batchProcessor.start(); err != nil {
			logger.Ctx(context.Background()).Error("重启批量处理器失败", zap.Error(err))
		}

		logger.Ctx(context.Background()).Info("存储重连成功")
		return
	}

	logger.Ctx(context.Background()).Error("存储重连失败，已达到最大重试次数",
		zap.Int("maxRetries", hc.maxRetries))
}
