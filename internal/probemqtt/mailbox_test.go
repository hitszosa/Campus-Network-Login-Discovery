package probemqtt

import (
	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"
	"testing"
)

func TestLatest(t *testing.T) {
	m := NewMailbox()
	for i := 0; i < 10000; i++ {
		m.Submit(probecore.Result{Reason: "old"})
	}
	m.Submit(probecore.Result{Reason: "latest"})
	if got := <-m.C; got.Reason != "latest" {
		t.Fatal(got)
	}
	if len(m.C) != 0 {
		t.Fatal("mailbox should be empty")
	}
}
