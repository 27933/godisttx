package pkg

import (
	"fmt"
	"sync"

	"github.com/xiaoxuxiansheng/redis_lock"
)

const (
	network  = "tcp"
	address  = ""
	password = ""
)

var (
	redisClient *redis_lock.Client
	once        sync.Once
)

// NewRedisClient 返回自定义的Redis客户端对象
func NewRedisClient(network, address, password string) *redis_lock.Client {
	return redis_lock.NewClient(network, address, password)
}

// GetRedisClient 返回全局的 redisClient 对象
// 如果 redisClient 未初始化，则使用 sync.Once 来初始化它，并返回已初始化的 redisClient
func GetRedisClient() *redis_lock.Client {
	once.Do(func() {
		redisClient = redis_lock.NewClient(network, address, password)
	})
	return redisClient
}

// BuildTXKey 构造事务 id key，用于幂等去重
func BuildTXKey(componentID, txID string) string {
	return fmt.Sprintf("txKey:%s:%s", componentID, txID)
}

// BuildTXDetailKey 构造事务事务细节 key
func BuildTXDetailKey(componentID, txID string) string {
	return fmt.Sprintf("txDetailKey:%s:%s", componentID, txID)
}

// BuildDataKey 构造请求 id，用于记录状态机
func BuildDataKey(componentID, txID, bizID string) string {
	return fmt.Sprintf("txKey:%s:%s:%s", componentID, txID, bizID)
}

// BuildTXLockKey 构造事务锁 key
func BuildTXLockKey(componentID, txID string) string {
	return fmt.Sprintf("txLockKey:%s:%s", componentID, txID)
}

// BuildTXRecordLockKey 返回一个构建事务记录锁的键
func BuildTXRecordLockKey() string {
	return "gotcc:txRecord:lock"
}
