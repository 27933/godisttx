package txmanager

import (
	"context"
	"time"

	"github.com/xiaoxuxiansheng/gotcc/component"
)

// TXStore 事务日志存储模块
// 1. 定义: 用于存储和管理日志明细记录的模块
// 2. 功能:
//  2.1 支持事务的明细数据的 CRUD 能力
//  2.2 底层需要应用到实际的存储组件作为支持
// 3. 该模块在整个框架中的 skd 体现为一个抽象的interface, 需要由用户完成具体类的实现, 并注入进 TXManager 中

// TXStore 事务日志存储模块
// 因为这个模块是偏向服务的, 所以需要用户自定义实现该模块
// 即在这里定义的是TXStore接口, 需要用户实现里面的方法从而实现该接口!
type TXStore interface {
	// CreateTX 创建一条事务明细记录
	// 注意: 这里返回的 txID 是在整个分布式架构下全局唯一的事务ID!
	CreateTX(ctx context.Context, components ...component.TCCComponent) (txID string, err error)
	// TXUpdate 更新事务进度：实际更新的是每个组件的 try 请求响应结果
	TXUpdate(ctx context.Context, txID string, componentID string, accept bool) error
	// TXSubmit 提交事务的最终状态, 标识事务执行结果为成功或失败
	TXSubmit(ctx context.Context, txID string, success bool) error
	// GetHangingTXs 获取到所有未完成的事务
	GetHangingTXs(ctx context.Context) ([]*Transaction, error)
	// GetTX 获取指定的一笔事务
	GetTX(ctx context.Context, txID string) (*Transaction, error)
	// Lock 锁住整个 TXStore 模块（要求为分布式锁） -> 多个 TX Manager 节点同时执行异步轮询操作在修改事务状态的时候可能会发生冲突
	Lock(ctx context.Context, expireDuration time.Duration) error
	// Unlock 解锁TXStore 模块
	Unlock(ctx context.Context) error
}
