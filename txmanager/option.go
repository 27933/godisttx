package txmanager

import "time"

// Options TX Manager 事务协调器中的一个字段, 保存一些配置信息
type Options struct {
	// 事务执行时长限制 避免长事务一直执行!
	Timeout time.Duration
	// 轮询监控任务间隔时长
	MonitorTick time.Duration
}

type Option func(*Options)

// WithTimeout 暴露接口返回设置事务执行时长的函数
func WithTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	return func(o *Options) {
		o.Timeout = timeout
	}
}

// WithMonitorTick 暴露接口返回设置轮询任务间隔时长的函数
func WithMonitorTick(tick time.Duration) Option {
	if tick <= 0 {
		tick = 10 * time.Second
	}

	return func(o *Options) {
		o.MonitorTick = tick
	}
}

// repair 要是没有设置轮询监控任务间隔时长和事务执行时长 就会赋值默认值
func repair(o *Options) {
	// 轮询监控任务间隔时长为10s
	if o.MonitorTick <= 0 {
		o.MonitorTick = 10 * time.Second
	}

	//  事务执行时长为5s
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
}
