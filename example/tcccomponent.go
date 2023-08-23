package example

import (
	"context"
	"errors"
	"fmt"

	"github.com/demdxx/gocast"
	"github.com/xiaoxuxiansheng/gotcc/component"
	"github.com/xiaoxuxiansheng/gotcc/example/pkg"
	"github.com/xiaoxuxiansheng/redis_lock"
)

// TXStatus tcc 组件侧记录的一笔事务的状态
type TXStatus string

func (t TXStatus) String() string {
	return string(t)
}

const (
	TXTried     TXStatus = "tried"     // 已执行 try 操作
	TXConfirmed TXStatus = "confirmed" // 已执行 confirm 操作
	TXCanceled  TXStatus = "canceled"  // 已执行 cancel 操作
)

// DataStatus 一笔事务对应数据的状态
type DataStatus string

func (d DataStatus) String() string {
	return string(d)
}

const (
	DataFrozen     DataStatus = "frozen"     // 冻结态
	DataSuccessful DataStatus = "successful" // 成功态
)

// MockComponent 内置 redis 客户端，用于完成一些状态数据的存取
type MockComponent struct {
	id     string // tcc 组件唯一标识 id，构造时由使用方传入
	client *redis_lock.Client
}

func NewMockComponent(id string, client *redis_lock.Client) *MockComponent {
	return &MockComponent{
		id:     id,
		client: client,
	}
}

// ID 返回 tcc 组件的唯一标识 id
func (m *MockComponent) ID() string {
	return m.id
}

func (m *MockComponent) Try(ctx context.Context, req *component.TCCReq) (*component.TCCResp, error) {
	// 1. 传入TCC 组件ID和 TX Manager 事务协调器ID并基于 txID 维度加锁
	lock := redis_lock.NewRedisLock(pkg.BuildTXLockKey(m.id, req.TXID), m.client)
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	defer func() {
		_ = lock.Unlock(ctx)
	}()

	// 2. 基于 txID 幂等性去重
	txStatus, err := m.client.Get(ctx, pkg.BuildTXKey(m.id, req.TXID)) // 返回对应于该事务ID和TCC 组件ID的操作状态 txStatus
	// 2.1 判断返回的错误不是该key对应value不存在的错误
	if err != nil && !errors.Is(err, redis_lock.ErrNil) {
		return nil, err
	}

	res := component.TCCResp{
		ComponentID: m.id,
		TXID:        req.TXID,
	}
	// 2.1 根据查询到的该事务的执行状态 txStatus 来判断是否是重复操作
	switch txStatus {
	case TXTried.String(), TXConfirmed.String(): // 重复的 try 请求，给予成功的响应
		res.ACK = true
		return &res, nil
	case TXCanceled.String(): // 先 cancel，后收到 try 请求，拒绝
		return &res, nil
	default:
	}

	// 3. 执行 try 操作，将数据状态置为 frozen
	// bizID 是实际操作的业务键, 即对数据实际操作的业务键
	bizID := gocast.ToString(req.Data["biz_id"])
	// m.id + req.TXID + bizID 通过这三样将此次业务操作置为冻结态!!!
	if _, err = m.client.Set(ctx, pkg.BuildTXDetailKey(m.id, req.TXID), bizID); err != nil {
		return nil, err
	}

	// 3.1 要求必须从零到一把 bizID 对应的数据置为冻结态
	// m.client.SetNX 在 Redis 中设置一个键值对，只有当键不存在时才会设置，并返回一个表示是否设置成功的值。
	reply, err := m.client.SetNX(ctx, pkg.BuildDataKey(m.id, req.TXID, bizID), DataFrozen.String())
	if err != nil {
		return nil, err
	}
	// 倘若数据此前已冻结或已使用，则拒绝本次 try 请求
	if reply != 1 {
		return &res, nil
	}

	// 3.2 在当前组件(Redis)下更新针对于该事务和该TCC 组件的状态
	_, err = m.client.Set(ctx, pkg.BuildTXKey(m.id, req.TXID), TXTried.String())
	if err != nil {
		return nil, err
	}

	// 4. try 请求执行成功
	res.ACK = true
	return &res, nil
}

func (m *MockComponent) Confirm(ctx context.Context, txID string) (*component.TCCResp, error) {
	// 1. 基于 txID 维度加锁
	lock := redis_lock.NewRedisLock(pkg.BuildTXLockKey(m.id, txID), m.client)
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	defer func() {
		_ = lock.Unlock(ctx)
	}()

	// 2. 校验事务状态, 要求 txID 此前状态为 tried
	txStatus, err := m.client.Get(ctx, pkg.BuildTXKey(m.id, txID))
	if err != nil {
		return nil, err
	}

	res := component.TCCResp{
		ComponentID: m.id,
		TXID:        txID,
	}
	switch txStatus {
	case TXConfirmed.String(): // 已 confirm，直接幂等响应为成功
		res.ACK = true
		return &res, nil
	case TXTried.String(): // 只有状态为 try 放行!!
	default: // 其他情况直接拒绝
		return &res, nil
	}

	// 获取事务对应的 bizID
	bizID, err := m.client.Get(ctx, pkg.BuildTXDetailKey(m.id, txID))
	if err != nil {
		return nil, err
	}

	// 3. 校验业务数据此前状态是否为冻结
	dataStatus, err := m.client.Get(ctx, pkg.BuildDataKey(m.id, txID, bizID))
	if err != nil {
		return nil, err
	}
	if dataStatus != DataFrozen.String() {
		// 非法的数据状态，拒绝
		return &res, nil
	}

	// 4. 把对应数据处理状态置为 successful
	if _, err = m.client.Set(ctx, pkg.BuildDataKey(m.id, txID, bizID), DataSuccessful.String()); err != nil {
		return nil, err
	}

	// 把事务状态更新为成功，这一步哪怕失败了也不阻塞主流程
	_, _ = m.client.Set(ctx, pkg.BuildTXKey(m.id, txID), TXConfirmed.String())

	// 5. 处理成功，给予成功的响应
	res.ACK = true
	return &res, nil
}

func (m *MockComponent) Cancel(ctx context.Context, txID string) (*component.TCCResp, error) {
	// 基于 txID 维度加锁
	lock := redis_lock.NewRedisLock(pkg.BuildTXLockKey(m.id, txID), m.client)
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	defer func() {
		_ = lock.Unlock(ctx)
	}()

	// 查看事务的状态，只要不是 confirmed，就无脑置为 canceld
	txStatus, err := m.client.Get(ctx, pkg.BuildTXKey(m.id, txID))
	if err != nil && !errors.Is(err, redis_lock.ErrNil) {
		return nil, err
	}
	// 先 confirm 后 cancel，属于非法的状态扭转链路
	if txStatus == TXConfirmed.String() {
		return nil, fmt.Errorf("invalid tx status: %s, txid: %s", txStatus, txID)
	}

	// 根据事务获取对应的 bizID
	bizID, err := m.client.Get(ctx, pkg.BuildTXDetailKey(m.id, txID))
	if err != nil && errors.Is(err, redis_lock.ErrNil) {
		return nil, err
	}

	if bizID != "" {
		// 删除对应的 frozen 冻结记录
		if err = m.client.Del(ctx, pkg.BuildDataKey(m.id, txID, bizID)); err != nil {
			return nil, err
		}
	}

	// 把事务状态更新为 canceld
	_, err = m.client.Set(ctx, pkg.BuildTXKey(m.id, txID), TXCanceled.String())
	if err != nil {
		return nil, err
	}

	return &component.TCCResp{
		ACK:         true,
		ComponentID: m.id,
		TXID:        txID,
	}, nil
}
