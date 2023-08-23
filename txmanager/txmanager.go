package txmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xiaoxuxiansheng/gotcc/component"
	"github.com/xiaoxuxiansheng/gotcc/log"
)

// TCC Manager 事务协调器  -> 封装成SDK(一组适合于开发人员的平台特定构建工具集)
// 1. 组成部分:
//  1.1 TXManager：事务协调器，class  -> 提供启动事务、注册组件、停止事务、两阶段提交、推进事务状态、异步轮询重试功能；
//  1.2 TXStore：事务日志存储模块，interface  -> 提供记录日志等相关工作
//  1.3 registryCenter：TCC 组件注册管理中心，class  -> 提供 TCC 组件注册、根据事务ID得到TCC 组件等功能
//  1.4 TCCComponent：TCC 组件，interface  -> 提供 Try、Confirm、Cancel 三个方法调用接口
// 2. TCC Manager功能
//  2.1 作为 gotcc 的统一入口，供使用方执行启动事务和注册组件的操作
//  2.2 作为中枢系统分别和 RegisterCenter、TXStore 交互
//  2.3 需要串联起整个 Try-Confirm/Canel 的 2PC 调用流程
//  2.4 需要运行异步轮询任务，推进未完成的事务走向终态

// TXManager 事务协调器
type TXManager struct {
	ctx            context.Context    // 用于反映 TXManager 运行生命周期的的 context，当 ctx 终止时，异步轮询任务也会随之退出
	stop           context.CancelFunc // 用于停止 txManager 的控制器. 当 stop 被调用后，异步轮询任务会被终止
	opts           *Options           // 内聚了一些 TXManager 的配置项，可以由使用方自定义，并通过 option 注入
	txStore        TXStore            // 内置的事务日志存储模块，需要由使用方实现并完成注入
	registryCenter *registryCenter    // TCC 组件的注册管理中心
}

// NewTXManager 初始化并返回事务协调器 - 构造器方法
func NewTXManager(txStore TXStore, opts ...Option) *TXManager {
	ctx, cancel := context.WithCancel(context.Background())
	txManager := TXManager{
		opts:           &Options{},
		txStore:        txStore,
		registryCenter: newRegistryCenter(),
		ctx:            ctx,
		stop:           cancel,
	}

	for _, opt := range opts {
		// txManager.opts 传进去自动赋值
		opt(txManager.opts)
	}

	repair(txManager.opts)

	// 在TxManager实例被构造出来就会伴生地启动异步轮询任务
	go txManager.run()
	return &txManager
}

func (t *TXManager) Stop() {
	t.stop()
}

func (t *TXManager) Register(component component.TCCComponent) error {
	return t.registryCenter.register(component)
}

// Transaction 用户启动分布式事务的入口
// -> reqs ...*RequestEntity 在入参中声明本次事务涉及到的组件以及需要在 Try 流程中传递给对应组件的请求参数
func (t *TXManager) Transaction(ctx context.Context, reqs ...*RequestEntity) (bool, error) {
	tctx, cancel := context.WithTimeout(ctx, t.opts.Timeout)
	defer cancel()

	// 1. 根据入参获得当前事务的所有的 TCC 组件
	componentEntities, err := t.getComponents(tctx, reqs...)
	if err != nil {
		return false, err
	}

	// 2. 创建事务明细记录，并取得全局唯一的事务 id
	txID, err := t.txStore.CreateTX(tctx, componentEntities.ToComponents()...)
	if err != nil {
		return false, err
	}

	// 3. 针对当前事务进行两阶段提交， try-confirm/cancel
	return t.twoPhaseCommit(ctx, txID, componentEntities)
}

// backOffTick 增加轮询时间间隔
// 每次对时间间隔进行翻倍, 封顶为初始时长的8倍
func (t *TXManager) backOffTick(tick time.Duration) time.Duration {
	tick <<= 1
	if threshold := t.opts.MonitorTick << 3; tick > threshold {
		return threshold
	}
	return tick
}

// run 异步轮询流程, 用于提高事务执行第二阶段的成功率.
//  1. 作用: 倘若存在事务已经完成第一阶段 Try 操作的执行，但是第二阶段没执行成功，
// 			  则需要由异步轮询流程进行兜底处理，为事务补齐第二阶段的操作，并将事务状态更新为终态
//  2. 实现方式: for循环 + select 多路复用 + 分布式锁
//	 2.1 select 多路复用保证当txManager事务协调器的ctx被关闭后能够及时的关闭异步轮询的goroutine
//   2.2 对 txStore 加分布式锁，避免分布式服务下多个 TX Manager 服务实例的轮询任务重复执行
func (t *TXManager) run() {
	var tick time.Duration
	var err error
	// for 循环自旋
	for {
		// 每一次循环都需要对 tick 重新赋值
		// 如果出现了失败，tick 需要避让，遵循退避策略增大 tick 间隔时长
		if err == nil {
			// 没有错误就赋值默认轮询监控任务间隔时长
			tick = t.opts.MonitorTick
		} else {
			// 如果处理过程中出现了错误，需要增长轮询时间间隔
			tick = t.backOffTick(tick)
		}
		select {
		// 当需要关闭的时候, 通过 t.ctx 传入关闭信息, 一旦收到关闭信息, 就会退出异步轮询任务
		case <-t.ctx.Done():
			return
		// time.After(tick)将在tick秒后发送信号, 即每隔tick秒后执行一次case后代码
		case <-time.After(tick):
			// 对 txStore 加分布式锁，避免分布式服务下多个 TX Manager 服务实例的轮询任务重复执行
			if err = t.txStore.Lock(t.ctx, t.opts.MonitorTick); err != nil {
				// 取锁失败时（大概率被其他TX Manager 服务实例占有），不对 tick 进行退避升级
				err = nil
				continue
			}

			// 获取仍然处于 hanging 状态(中间状态)的事务(注意这里是事务本身, 而不是事务ID)
			var txs []*Transaction
			// 所有的事务状态根据事务日志中记录的事务来获取
			// 日志中的事务状态是上一次轮询推进过程中剩下的处于 hanging 状态的事务!
			if txs, err = t.txStore.GetHangingTXs(t.ctx); err != nil {
				// 获取出错的话, 就关闭锁等待下一次的异步调用
				_ = t.txStore.Unlock(t.ctx)
				continue
			}

			err = t.batchAdvanceProgress(txs)
			_ = t.txStore.Unlock(t.ctx)
		}
	}
}

// batchAdvanceProgress 批量推进处于中间态的任务
// 如果推进每个处于中间态的事务的过程中, 出现错误的话, 只会返回发生的第一个错误
func (t *TXManager) batchAdvanceProgress(txs []*Transaction) error {
	// 对每笔事务进行状态推进
	errCh := make(chan error)
	// 另起一个goroutine 推进所有处于中间态的事务
	go func() {
		// 并发执行，推进各比事务的进度
		var wg sync.WaitGroup
		for _, tx := range txs {
			// shadow
			tx := tx
			wg.Add(1)
			// 对于每笔事务都启动 goroutine 进行该事务下所有 TCC 组件的重试操作
			go func() {
				defer wg.Done()
				if err := t.advanceProgress(tx); err != nil {
					// 遇到错误则投递到 errCh
					errCh <- err
				}
			}()
		}
		// 所有事务的推进操作结束才能继续执行
		wg.Wait()
		// 最后关闭 errCh 通道
		close(errCh)
	}()

	var firstErr error
	// 当 errCh 通道中没有错误的时候会发生阻塞
	// 直到 errCh 通道关闭过后, 才会继续执行(退出for循环)
	// 父 goroutine 通过 chan 阻塞在这里, 直到所有 goroutine 执行完成关闭 channel 通道才会继续执行
	for err := range errCh {
		// 记录遇到的第一个错误
		if firstErr != nil {
			continue
		}
		firstErr = err
	}

	return firstErr
}

// advanceProgressByTXID 传入一个事务 id 推进其进度
func (t *TXManager) advanceProgressByTXID(txID string) error {
	// 根据 txID 事务ID从事务日志中获取该事务的日志记录
	tx, err := t.txStore.GetTX(t.ctx, txID)
	if err != nil {
		return err
	} //
	return t.advanceProgress(tx)
}

// advanceProgress 传入一个事务推进其进度
// 传入的事务是在上一次轮询调度的时候是 hanging 的状态, 这里需要判断这些事务是否有所更新
func (t *TXManager) advanceProgress(tx *Transaction) error {
	// 1. 根据各个 component try 请求的情况，推断出事务当前的状态
	// 				当前事务的 TCC 组件状态                       <->         当前事务状态
	//              所有 TCC 组件Try操作都成功      TrySuccessful <->      成功       TXSuccessful
	//    所有 TCC 组件Try操作有一个处于 hanging 状态 TryHanging    <->      hanging   TXHanging
	//           所有 TCC 组件Try操作有一个失败       TryFailure   <->      失败       TXFailure

	// time.Now().Add(-t.opts.Timeout) 当前时间减去事务最长执行时间 -> 当前事务最早创建的时间
	txStatus := tx.getStatus(time.Now().Add(-t.opts.Timeout))
	// 1.1 当前事务状态为 hanging (表示存在 TCC 组件状态为 hanging) 的暂时不处理 等待下一轮询推进的时候再处理
	if txStatus == TXHanging {
		return nil
	}

	success := txStatus == TXSuccessful
	var confirmOrCancel func(ctx context.Context, component component.TCCComponent) (*component.TCCResp, error)
	var txAdvanceProgress func(ctx context.Context) error
	// 1.2 当前事务状态为 successful (表示所有 TCC 组件状态都是successful), 就需要推进 Confirm 操作
	// 1.3 当前事务状态为 failure (表示所有 TCC 组件状态都是successful), 就需要推进 Cancel 操作
	// 根据事务是否成功，定制不同的处理函数以供后续调用!
	if success {
		confirmOrCancel = func(ctx context.Context, component component.TCCComponent) (*component.TCCResp, error) {
			// 对 component 进行第二阶段的 confirm 操作
			return component.Confirm(ctx, tx.TXID)
		}
		txAdvanceProgress = func(ctx context.Context) error {
			// 更新事务日志记录的状态为成功
			return t.txStore.TXSubmit(ctx, tx.TXID, true)
		}

	} else {
		confirmOrCancel = func(ctx context.Context, component component.TCCComponent) (*component.TCCResp, error) {
			// 对 component 进行第二阶段的 cancel 操作
			return component.Cancel(ctx, tx.TXID)
		}

		txAdvanceProgress = func(ctx context.Context) error {
			// 更新事务日志记录的状态为失败
			return t.txStore.TXSubmit(ctx, tx.TXID, false)
		}
	}

	// 2. 遍历该事务的所有 TCC 组件执行第二阶段的动作
	for _, component := range tx.Components {
		// 2.1 根据 TXManager 事务协调器中事务对应 TCC 组件ID 获取实际的对应的 TCC component
		components, err := t.registryCenter.getComponents(component.ComponentID)
		if err != nil || len(components) == 0 {
			return errors.New("get tcc component failed")
		}
		// 2.2 执行二阶段的 confirm 或者 cancel 操作
		resp, err := confirmOrCancel(t.ctx, components[0])
		if err != nil {
			return err
		}
		if !resp.ACK {
			return fmt.Errorf("component: %s ack failed", component.ComponentID)
		}
	}

	// 3. 二阶段操作都执行完成后，对事务状态进行提交
	return txAdvanceProgress(t.ctx)
}

func (t *TXManager) twoPhaseCommit(ctx context.Context, txID string, componentEntities ComponentEntities) (bool, error) {
	// 1. 创建子 context 用于管理子 goroutine 生命周期
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error)
	// 2. 并发启动，批量执行各 tcc 组件的 try 流程
	go func() {
		var wg sync.WaitGroup
		for _, componentEntity := range componentEntities {
			// shadow
			componentEntity := componentEntity
			wg.Add(1)
			// 2.1 针对当前组件需要另起一个协程来启动 Try 操作
			go func() {
				defer wg.Done()
				// 2.2 当前组件执行 Try 操作
				resp, err := componentEntity.Component.Try(cctx, &component.TCCReq{
					ComponentID: componentEntity.Component.ID(),
					TXID:        txID,
					Data:        componentEntity.Request,
				})
				// 2.3 但凡有一个 component try 报错或者拒绝，那么整个事务都需要 cancel 的，但会放在 advanceProgressByTXID 流程处理
				if err != nil || !resp.ACK {
					log.ErrorContextf(cctx, "tx try failed, tx id: %s, comonent id: %s, err: %v", txID, componentEntity.Component.ID(), err)
					// 2.3.1 对对应的事务进行更新
					if _err := t.txStore.TXUpdate(cctx, txID, componentEntity.Component.ID(), false); _err != nil {
						log.ErrorContextf(cctx, "tx updated failed, tx id: %s, component id: %s, err: %v", txID, componentEntity.Component.ID(), _err)
					}
					// 2.3.2 将当前错误传入通道, 而一旦for循环执行一次就会调用 cancel 方法熔断其他所有子 goroutine 流程!
					errCh <- fmt.Errorf("component: %s try failed", componentEntity.Component.ID())
					return
				}
				// 2.4 try 请求成功，但是请求结果更新到事务日志失败时，也需要视为处理失败
				if err = t.txStore.TXUpdate(cctx, txID, componentEntity.Component.ID(), true); err != nil {
					log.ErrorContextf(cctx, "tx updated failed, tx id: %s, component id: %s, err: %v", txID, componentEntity.Component.ID(), err)
					errCh <- err
				}
			}()
		}

		wg.Wait()
		// 关闭 errCh，告知父 goroutine 所有任务已运行完成的信息
		close(errCh)
	}()

	successful := true
	// 3. 同样通过 channel 来进行阻塞, 直到channel 被关闭为止
	// 3.1 一旦有错误传入通道, for循环执行一次就会调用 cancel 方法熔断其他所有子 goroutine 流程!
	// 3.2 若是通道为空就会一直阻塞, 直到所有goroutine都执行完毕关闭该通道才会退出for循环继续执行
	if err := <-errCh; err != nil {
		// 只要有一笔 try 请求出现问题，其他的都进行终止
		// 这里关闭 cctx 是怎么实现控制子goroutine的呢???
		cancel()
		successful = false
	}

	// 4. 根据事务ID推进当前事务异步执行第二阶段(Confirm或者Cancel)
	// 之所以是异步，是因为实际上在第一阶段 try 的响应结果尘埃落定时，对应事务的成败已经有了定论
	// 第二阶段能够容忍异步执行的原因在于，执行失败时，还有轮询任务进行兜底
	go t.advanceProgressByTXID(txID)
	return successful, nil
}

// 并发执行，只要中间某次出现了失败，直接终止流程进行 cancel

// 如果全量执行成功，则返回成功的 ack，然后批量执行 confirm

func (t *TXManager) getComponents(ctx context.Context, reqs ...*RequestEntity) (ComponentEntities, error) {
	if len(reqs) == 0 {
		return nil, errors.New("emtpy task")
	}

	// 1. 调一下接口，确认这些都是合法的
	idToReq := make(map[string]*RequestEntity, len(reqs))
	componentIDs := make([]string, 0, len(reqs))
	// 1.1 去重, 保证一个事务里面每一个 TCC 组件都是唯一不可重复的, 同时也保存 TCC 组件ID和 TCC 组件的映射关系
	for _, req := range reqs {
		if _, ok := idToReq[req.ComponentID]; ok {
			return nil, fmt.Errorf("repeat component: %s", req.ComponentID)
		}
		idToReq[req.ComponentID] = req
		componentIDs = append(componentIDs, req.ComponentID)
	}

	// 2. 校验其合法性
	// 2.1 根据 TCC 组件事务ID列表 componentIDs 获得TCC 组件列表
	components, err := t.registryCenter.getComponents(componentIDs...)
	if err != nil {
		return nil, err
	}
	// 2.2 校验当前获得的 TCC 组件列表和 TX Manager 事务协调器中的 TCC 组件列表长度是否一致
	if len(componentIDs) != len(components) {
		return nil, errors.New("invalid componentIDs ")
	}

	// 3. 拼接 TCC 组件实体得到一个TCC 组件实体列表
	entities := make(ComponentEntities, 0, len(components))
	for _, component := range components {
		entities = append(entities, &ComponentEntity{
			Request:   idToReq[component.ID()].Request,
			Component: component,
		})
	}

	return entities, nil
}
