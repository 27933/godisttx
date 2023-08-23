package component

import "context"

// TCC Component TCC 组件模块
// 1. 定义: 定义该组件的接口, 需要用户自定义实现接口中的方法从而实现该接口
// 2. 使用流程:
// 2.1 在 TX Manager 启动时将其注册到 TX Manager 的注册中心 RegistryCenter 中
// 2.2 业务调用方调用 TX Manager 开启事务时, 通过 TX Manager 的注册中心 RegistryCenter 获取这些 TCC 组件, 并对其进行使用
// 3. 参数格式封装
// 3.1 TCCReq 请求参数  即 TX Manager 事务协调器调用 TCC 组件所需要的请求参数
// 3.2 TCCResp 响应结果 同样是 TX Manager 事务协调器调用 TCC 组件所返回的响应结果

// TCCReq 请求参数
type TCCReq struct {
	ComponentID string `json:"componentID"`
	// 全局唯一的事务 id
	TXID string                 `json:"txID"`
	Data map[string]interface{} `json:"data"`
}

// TCCResp 响应结果
type TCCResp struct {
	ComponentID string `json:"componentID"`
	ACK         bool   `json:"ack"`
	TXID        string `json:"txID"`
}

// TCCComponent 组件
// 用户需要自己实现的TCCComponent接口
type TCCComponent interface {
	// ID 返回组件唯一 id
	ID() string
	// Try 执行第一阶段的 try 操作
	Try(ctx context.Context, req *TCCReq) (*TCCResp, error)
	// Confirm 执行第二阶段的 confirm 操作
	Confirm(ctx context.Context, txID string) (*TCCResp, error)
	// Cancel 执行第二阶段的 cancel 操作
	Cancel(ctx context.Context, txID string) (*TCCResp, error)
}
