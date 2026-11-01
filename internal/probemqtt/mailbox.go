package probemqtt

import "github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"

// 只有串行探测循环调用 Submit；worker 只接收。
type Mailbox struct{ C chan probecore.Result }

func NewMailbox() *Mailbox { return &Mailbox{C: make(chan probecore.Result, 1)} }

func (m *Mailbox) Submit(r probecore.Result) {
	select {
	case m.C <- r:
		return
	default:
	}
	select {
	case <-m.C:
	default:
	} // 有旧结果就取走
	m.C <- r // 单一发送者；现在必有空位
}
