package integration

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

func TestLifecycleAndRetained(t *testing.T) {
	l := newLab(t, "")
	broker := "tcp://" + l.address
	observer := observe(t, broker, nil, "", "")
	agent := l.agent(broker, "")
	online := observer.availability(t, "online", "connected", "")
	latest := observer.report(t, func(r probemqtt.Report) bool { return r.ConnectionID == online.ConnectionID })
	if latest.SchemaVersion != 1 || latest.Sequence == 0 || latest.ValidForSeconds != 2 || latest.NodeID != "lab-01" {
		t.Fatal(latest)
	}
	if latest.Labels["network.campus/external_access"] != "unknown" {
		t.Fatal(latest)
	}
	cases := []struct{ mode, auth, reason string }{
		{"no", "false", "explicit_unauthenticated"}, {"portal", "false", "known_portal"},
		{"redirect", "unknown", "unrecognized_response"}, {"slow", "unknown", "timeout"},
		{"large", "unknown", "body_too_large"}, {"unexpected", "unknown", "unrecognized_response"}, {"yes", "true", "login_ok"},
	}
	for _, tc := range cases {
		l.mode.Store(tc.mode)
		previous := latest.Sequence
		latest = observer.report(t, func(r probemqtt.Report) bool { return r.Sequence > previous && r.Reason == tc.reason })
		if latest.Labels["network.campus/authenticated"] != tc.auth {
			t.Fatal(latest)
		}
	}
	// A new subscriber receives the existing retained report and online status.
	fresh := observe(t, broker, nil, "", "")
	fresh.report(t, func(r probemqtt.Report) bool { return r.ConnectionID == latest.ConnectionID })
	fresh.availability(t, "online", "connected", latest.ConnectionID)
	agent.stop(syscall.SIGTERM)
	if agent.err != nil {
		t.Fatal(agent.err)
	}
	observer.availability(t, "offline", "graceful_shutdown", latest.ConnectionID)
	fresh = observe(t, broker, nil, "", "")
	fresh.availability(t, "offline", "graceful_shutdown", latest.ConnectionID)
	agent = l.agent(broker, "")
	restarted := observer.report(t, func(r probemqtt.Report) bool { return r.ConnectionID != latest.ConnectionID })
	agent.stop(syscall.SIGKILL)
	observer.availability(t, "offline", "connection_lost", restarted.ConnectionID)
	fresh = observe(t, broker, nil, "", "")
	fresh.availability(t, "offline", "connection_lost", restarted.ConnectionID)
}

func TestBlackholeAndOfflinePublishFailure(t *testing.T) {
	l := newLab(t, "")
	observer := observe(t, "tcp://"+l.address, nil, "", "")
	proxy, drop := blackhole(t, l.address)
	agent := l.agent(proxy, "")
	first := observer.report(t, func(r probemqtt.Report) bool { return r.Reason == "login_ok" })
	drop.Store(true)
	observer.availability(t, "offline", "connection_lost", first.ConnectionID)
	// HTTP results continue to change while MQTT cannot connect.
	l.mode.Store("no")
	eventually(t, func() bool { return logsContain(agent, "auth=false") })
	recoveredAfter := time.Now()
	drop.Store(false)
	second := observer.report(t, func(r probemqtt.Report) bool {
		return r.ConnectionID != first.ConnectionID && r.Labels["network.campus/authenticated"] == "false"
	})
	if second.ObservedAt.Before(recoveredAfter) {
		t.Fatal("replayed an observation from before recovery")
	}
	drop.Store(true)
	agent.cmd.Process.Signal(syscall.SIGTERM)
	eventually(t, func() bool { return logsContain(agent, "offline not confirmed") })
	observer.availability(t, "offline", "connection_lost", second.ConnectionID)
	agent.stop(syscall.SIGTERM)
}

func TestBrokerRestartAndOldMailbox(t *testing.T) {
	l := newLab(t, "")
	broker := "tcp://" + l.address
	observer := observe(t, broker, nil, "", "")
	agent := l.agent(broker, "")
	first := observer.report(t, func(r probemqtt.Report) bool { return r.Reason == "login_ok" })
	watcher := start(t, nil, watchBinary, "-broker", broker)
	eventually(t, func() bool { return logsContain(watcher, "auth=true") })
	l.broker.stop(syscall.SIGTERM)
	eventually(t, func() bool { return logsContain(watcher, "unknown:") })
	l.mode.Store("no")
	eventually(t, func() bool { return logsContain(agent, "auth=false") })
	restartAt := time.Now()
	l.restartBroker()
	observer = observe(t, broker, nil, "", "")
	next := observer.report(t, func(r probemqtt.Report) bool {
		return r.ConnectionID != first.ConnectionID && r.Reason == "explicit_unauthenticated"
	})
	if next.ObservedAt.Before(restartAt) {
		t.Fatal("old mailbox result relabeled as new connection")
	}
	eventually(t, func() bool { return logsContain(watcher, "auth=false") })
}

func TestTTLBeforeWill(t *testing.T) {
	l := newLab(t, "")
	broker := "tcp://" + l.address
	observer := observe(t, broker, nil, "", "")
	// Use a long Keep Alive so report expiry happens before LWT.
	path := filepath.Join(t.TempDir(), "agent.yaml")
	agent := l.agent(broker, "")
	first := observer.report(t, func(r probemqtt.Report) bool { return r.Reason == "login_ok" })
	// Reuse the generated configuration with only Keep Alive changed.
	original := agent.cmd.Args[len(agent.cmd.Args)-1]
	data, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	agent.stop(syscall.SIGTERM)
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(data), "keep_alive: 2s", "keep_alive: 60s")), 0600); err != nil {
		t.Fatal(err)
	}
	agent = start(t, nil, agentBinary, "-config", path)
	latest := observer.report(t, func(r probemqtt.Report) bool { return r.Reason == "login_ok" && r.ConnectionID != first.ConnectionID })
	watcher := start(t, nil, watchBinary, "-broker", broker)
	eventually(t, func() bool { return logsContain(watcher, "auth=true") })
	if err := agent.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	// The watch timer, not a new MQTT message, makes the view unknown.
	eventually(t, func() bool {
		body, _ := os.ReadFile(watcher.log)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasSuffix(line, " unknown") {
				return true
			}
		}
		return false
	})
	retained := observe(t, broker, nil, "", "")
	retained.availability(t, "online", "connected", latest.ConnectionID)
	agent.cmd.Process.Signal(syscall.SIGCONT)
	// Stop the consumer cleanly; broker-loss recovery is checked in TestBrokerRestartAndOldMailbox.
	watcher.cmd.Process.Signal(syscall.SIGTERM)
	watcher.stop(syscall.SIGTERM)
	if watcher.err != nil {
		t.Fatal(watcher.err)
	}
}
