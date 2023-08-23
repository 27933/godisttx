package txmanager

import (
	"errors"
	"fmt"
	"github.com/xiaoxuxiansheng/gotcc/component"
	"sync"
)

// TX Manager 中 RegistryCenter 模块
// 1. 通过map存储所有注册进来的 TCC 组件ID和实际的 TCC 组件的映射！
// 2. 通过读写锁 rwMutex 保护map的并发安全性
// 3. 提供注册和查询 TCC 组件的功能

type registryCenter struct {
	mux        sync.RWMutex
	components map[string]component.TCCComponent
}

// newRegistryCenter 构造 TXManager 的注册中心结构体
func newRegistryCenter() *registryCenter {
	return &registryCenter{
		//mux: new(sync.RWMutex)
		components: make(map[string]component.TCCComponent),
	}
}

// register 将 TCC 组件注册进入注册中心的map中, 存储 TCC 组件ID和 TCC 组件之间的映射关系
// 提供接口给 TCC 组件注册时进行调用, 实现 TCC 组件自己注册进入 TX Manager 事务协调器的目的
func (r *registryCenter) register(component component.TCCComponent) error {
	// 1. 通过 rwMutex 维护 map 的并发安全性
	r.mux.Lock()
	defer r.mux.Unlock()
	// 2. 不能有重复的TCC 组件ID
	if _, ok := r.components[component.ID()]; ok {
		return errors.New("repeat component id")
	}
	// 3. 保存
	r.components[component.ID()] = component
	return nil
}

// getComponents 上游 TX Manager 通过事务ID获得对应的多个TCC组件实例!
// 同样是暴露接口给上游调用
func (r *registryCenter) getComponents(componentIDs ...string) ([]component.TCCComponent, error) {
	components := make([]component.TCCComponent, 0, len(componentIDs))

	r.mux.RLock()
	defer r.mux.RUnlock()

	for _, componentID := range componentIDs {
		component, ok := r.components[componentID]
		if !ok {
			return nil, fmt.Errorf("component id: %s not existed", componentID)
		}
		components = append(components, component)
	}

	return components, nil
}
