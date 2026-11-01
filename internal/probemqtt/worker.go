package probemqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"time"
)

type Worker struct {
	*Mailbox
	Config   Config
	Node     string
	ValidFor time.Duration
	TLS      *tls.Config
	Trigger  chan<- struct{}
}

func (w *Worker) once(ctx context.Context) (bool, error) {
	c := w.Config
	s, err := newSession(ctx, c, w.Node, w.TLS)
	if err != nil {
		return false, err
	}
	defer s.abort()
	if err := wait(ctx, s.client.Connect(), c.PublishTimeout); err != nil {
		return false, err
	}
	if err := s.availability(ctx, c, w.Node, "online", "connected"); err != nil {
		return false, err
	}
	s.readyAt = time.Now() // 保留单调时钟部分，只接收此后开始采样的结果
	log.Printf("node=%s mqtt online connection_id=%s", w.Node, s.id)
	select {
	case w.Trigger <- struct{}{}:
	default:
	}
	for {
		if ctx.Err() != nil {
			w.shutdown(s)
			return true, nil
		}
		select {
		case <-ctx.Done():
			w.shutdown(s)
			return true, nil
		case <-s.lost:
			return true, errors.New("MQTT connection lost")
		case r := <-w.C:
			if r.ObservedAt.Before(s.readyAt) || !time.Now().Before(r.ObservedAt.Add(w.ValidFor)) {
				continue
			}
			s.sequence++
			report := Report{1, w.Node, s.id, s.sequence, r.ObservedAt.UTC(), int64(w.ValidFor / time.Second),
				r.Labels(), r.Reason, r.Duration.Milliseconds()}
			if err := s.publish(ctx, c, c.TopicPrefix+"/"+w.Node+"/report", report); err != nil {
				if ctx.Err() != nil {
					w.shutdown(s)
				}
				return true, err
			}
		}
	}
}

func (w *Worker) shutdown(s *session) {
	// runProbe 与 worker 收到同一取消信号；本 worker 从此不再消费报告。
	ctx, cancel := context.WithTimeout(context.Background(), w.Config.PublishTimeout)
	defer cancel()
	if err := s.availability(ctx, w.Config, w.Node, "offline", "graceful_shutdown"); err != nil {
		log.Printf("node=%s offline not confirmed; closing transport", w.Node)
		return // defer s.abort() 直接关闭 socket，留给 Broker 发 Will
	}
	s.client.Disconnect(100) // 只在 offline 已确认后正常断开
}
