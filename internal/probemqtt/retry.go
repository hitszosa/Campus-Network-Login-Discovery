package probemqtt

import (
	"context"
	"log"
	"math/rand/v2"
	"time"
)

func (w *Worker) Run(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		connected, err := w.once(ctx)
		if ctx.Err() != nil {
			return
		}
		if connected {
			delay = time.Second
		}
		log.Printf("node=%s mqtt retry_in<=%s error=%v", w.Node, delay, err)
		// 等待时间在 [delay/2, delay] 内；连续失败时翻倍，到 MaxBackoff 为止。
		pause := delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, w.Config.MaxBackoff)
	}
}
