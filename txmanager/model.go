package txmanager

import (
	"time"

	"github.com/xiaoxuxiansheng/gotcc/component"
)

type RequestEntity struct {
	// 组件名称
	ComponentID string `json:"componentName"`
	// 组件入参 -> Try 请求时传递的参数
	Request map[string]interface{} `json:"request"`
}

type ComponentEntities []*ComponentEntity

func (c ComponentEntities) ToComponents() []component.TCCComponent {
	components := make([]component.TCCComponent, 0, len(c))
	for _, entity := range c {
		components = append(components, entity.Component)
	}
	return components
}

type ComponentEntity struct {
	Request   map[string]interface{}
	Component component.TCCComponent
}

// 事务状态
type TXStatus string

const (
	// 事务执行中
	TXHanging TXStatus = "hanging"
	// 事务成功
	TXSuccessful TXStatus = "successful"
	// 事务失败
	TXFailure TXStatus = "failure"
)

func (t TXStatus) String() string {
	return string(t)
}

type ComponentTryStatus string

func (c ComponentTryStatus) String() string {
	return string(c)
}

const (
	TryHanging ComponentTryStatus = "hanging"
	// 事务成功
	TrySucceesful ComponentTryStatus = "successful"
	// 事务失败
	TryFailure ComponentTryStatus = "failure"
)

type ComponentTryEntity struct {
	ComponentID string
	TryStatus   ComponentTryStatus
}

// 事务
type Transaction struct {
	TXID       string `json:"txID"`
	Components []*ComponentTryEntity
	Status     TXStatus  `json:"status"`
	CreatedAt  time.Time `json:"createdAt"`
}

func NewTransaction(txID string, componentEntities ComponentEntities) *Transaction {
	entities := make([]*ComponentTryEntity, 0, len(componentEntities))
	for _, componentEntity := range componentEntities {
		entities = append(entities, &ComponentTryEntity{
			ComponentID: componentEntity.Component.ID(),
		})
	}
	return &Transaction{
		TXID:       txID,
		Components: entities,
	}
}

// getStatus 获取事务的状态
func (t *Transaction) getStatus(createdBefore time.Time) TXStatus {
	// 1 判断当前事务是否超时, 如果事务超时了，都还未被置为成功，直接置为失败
	// t.CreatedAt.Before(createdBefore)
	// 要是 t.CreatedAt 比 createdBefore小的话返回true 否则返回false
	if t.CreatedAt.Before(createdBefore) {
		return TXFailure
	}

	// 2. 遍历判断当前事务的所有 TCC 组件是否有失败的, 一旦存在一个就把状态置为失败
	var hangingExist bool
	for _, component := range t.Components {
		if component.TryStatus == TryFailure {
			return TXFailure
		}
		// 若是当前事务中存在 hanging 状态的 TCC 组件就一直保持
		hangingExist = hangingExist || (component.TryStatus != TrySucceesful)
	}

	// 如果存在组件 try 操作没执行成功，则返回 hanging 状态
	if hangingExist {
		return TXHanging
	}
	return TXSuccessful
}
