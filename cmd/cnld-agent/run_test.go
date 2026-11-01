package main

import (
	"context"
	"testing"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"
)

type recorder struct{ C chan probecore.Result }

func (r recorder) Submit(v probecore.Result) { r.C <- v }

func TestRunProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trigger := make(chan struct{}, 1)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	output := recorder{make(chan probecore.Result, 2)}
	done := make(chan struct{})
	sample := func(ctx context.Context) probecore.Result {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return probecore.Result{Authenticated: probecore.AuthYes}
	}
	go func() { defer close(done); runProbe(ctx, time.Hour, trigger, sample, output) }()
	<-entered             // 启动即探测；第一次仍在执行
	trigger <- struct{}{} // 重连通知留在 channel，等第一次结束再处理
	release <- struct{}{}
	<-output.C
	<-entered // 不用等一小时，通知立即触发第二次
	cancel()  // 第二次采样可被取消，且不会提交第二份报告
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not stop")
	}
	if len(output.C) != 0 {
		t.Fatal("published after cancel")
	}
}
