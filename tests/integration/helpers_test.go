package integration

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

var agentBinary, watchBinary string

func TestMain(m *testing.M) {
	if os.Getenv("CNLD_INTEGRATION") != "1" {
		os.Exit(m.Run())
	}
	if _, err := exec.LookPath("mosquitto"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "cnld-integration-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, name := range []string{"cnld-agent", "cnld-watch"} {
		cmd := exec.Command("go", "build", "-race", "-o", filepath.Join(dir, name), "./cmd/"+name)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s %v\n", name, out, err)
			os.RemoveAll(dir)
			os.Exit(1)
		}
	}
	agentBinary, watchBinary = filepath.Join(dir, "cnld-agent"), filepath.Join(dir, "cnld-watch")
	result := m.Run()
	os.RemoveAll(dir)
	os.Exit(result)
}

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CNLD_INTEGRATION") != "1" {
		t.Skip("set CNLD_INTEGRATION=1 and install mosquitto")
	}
}

type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  string
}

func start(t *testing.T, env []string, name string, args ...string) *process {
	t.Helper()
	path := filepath.Join(t.TempDir(), "process.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(name, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = file, file
	if err := cmd.Start(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	p := &process{cmd: cmd, done: make(chan struct{}), log: path}
	go func() { p.err = cmd.Wait(); file.Close(); close(p.done) }()
	t.Cleanup(func() {
		p.stop(syscall.SIGCONT)
		if t.Failed() {
			body, _ := os.ReadFile(path)
			t.Logf("%s logs:\n%s", name, body)
		}
	})
	return p
}

func (p *process) stop(first syscall.Signal) {
	select {
	case <-p.done:
		return
	default:
	}
	if first == syscall.SIGCONT {
		p.cmd.Process.Signal(syscall.SIGCONT)
		first = syscall.SIGTERM
	}
	p.cmd.Process.Signal(first)
	select {
	case <-p.done:
	case <-time.After(4 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(12 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("condition timed out")
		case <-ticker.C:
		}
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}

type lab struct {
	t                 *testing.T
	dir, address, url string
	mode              atomic.Value
	broker            *process
}

func newLab(t *testing.T, extra string) *lab {
	t.Helper()
	requireIntegration(t)
	l := &lab{t: t, dir: t.TempDir(), address: freeAddress(t)}
	l.mode.Store("yes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch l.mode.Load().(string) {
		case "yes":
			io.WriteString(w, "authenticated")
		case "no":
			io.WriteString(w, "unauthenticated")
		case "portal":
			http.Redirect(w, r, "http://login.example.edu/", 302)
		case "redirect":
			http.Redirect(w, r, "http://elsewhere.example.edu/", 302)
		case "slow":
			<-r.Context().Done()
		case "large":
			w.Write(make([]byte, 8192))
		default:
			io.WriteString(w, "unexpected")
		}
	}))
	t.Cleanup(server.Close)
	l.url = server.URL
	_, port, _ := net.SplitHostPort(l.address)
	if extra == "" {
		extra = "allow_anonymous true\npersistence false\n"
	}
	config := fmt.Sprintf("listener %s 127.0.0.1\nlog_type all\n", port) + extra
	if err := os.WriteFile(filepath.Join(l.dir, "mosquitto.conf"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	l.restartBroker()
	return l
}

func (l *lab) restartBroker() {
	l.broker = start(l.t, nil, "mosquitto", "-c", filepath.Join(l.dir, "mosquitto.conf"))
	eventually(l.t, func() bool {
		conn, err := net.DialTimeout("tcp", l.address, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	})
}

func (l *lab) agent(broker string, extra string, env ...string) *process {
	l.t.Helper()
	path := filepath.Join(l.t.TempDir(), "agent.yaml")
	config := fmt.Sprintf(`node_id: lab-01
sinks: [mqtt]
probe:
  endpoint: %s
  interval: 200ms
  timeout: 100ms
  valid_for: 2s
  authenticated_rule: {status_code: 200, body_equals: authenticated}
  unauthenticated_rule: {status_code: 200, body_equals: unauthenticated}
  portal_hosts: [login.example.edu]
mqtt:
  broker: %s
  keep_alive: 2s
  publish_timeout: 200ms
  max_backoff: 1s
`, l.url, broker) + extra
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		l.t.Fatal(err)
	}
	return start(l.t, env, agentBinary, "-config", path)
}

type message struct {
	topic string
	body  []byte
}
type observer struct {
	client   mqtt.Client
	messages chan message
	reports  map[string]probemqtt.Report
	states   map[string]probemqtt.Availability
}

func observe(t *testing.T, broker string, tlsConfig *tls.Config, username, password string) *observer {
	t.Helper()
	o := &observer{messages: make(chan message, 256), reports: make(map[string]probemqtt.Report), states: make(map[string]probemqtt.Availability)}
	options := mqtt.NewClientOptions().AddBroker(broker).SetClientID(fmt.Sprintf("test-%d", time.Now().UnixNano())).
		SetProtocolVersion(4).SetAutoReconnect(false).SetConnectRetry(false).SetConnectTimeout(time.Second).
		SetTLSConfig(tlsConfig).SetUsername(username).SetPassword(password)
	o.client = mqtt.NewClient(options)
	token := o.client.Connect()
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("observer connect: %v", token.Error())
	}
	t.Cleanup(func() { o.client.Disconnect(50) })
	token = o.client.Subscribe("cnld/nodes/+/+", 1, func(_ mqtt.Client, m mqtt.Message) {
		select {
		case o.messages <- message{m.Topic(), append([]byte(nil), m.Payload()...)}:
		default:
		}
	})
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}
	return o
}

func (o *observer) ingest(msg message) {
	if strings.HasSuffix(msg.topic, "/report") {
		var r probemqtt.Report
		if json.Unmarshal(msg.body, &r) == nil {
			o.reports[msg.topic] = r
		}
	} else {
		var a probemqtt.Availability
		if json.Unmarshal(msg.body, &a) == nil {
			o.states[msg.topic] = a
		}
	}
}

func (o *observer) report(t *testing.T, predicate func(probemqtt.Report) bool) probemqtt.Report {
	t.Helper()
	timer := time.NewTimer(12 * time.Second)
	defer timer.Stop()
	for {
		for _, r := range o.reports {
			if predicate(r) {
				return r
			}
		}
		select {
		case <-timer.C:
			t.Fatal("report timed out")
		case msg := <-o.messages:
			o.ingest(msg)
		}
	}
}

func (o *observer) availability(t *testing.T, status, reason, id string) probemqtt.Availability {
	t.Helper()
	timer := time.NewTimer(12 * time.Second)
	defer timer.Stop()
	for {
		for _, a := range o.states {
			if a.Status == status && (reason == "" || a.Reason == reason) && (id == "" || a.ConnectionID == id) {
				return a
			}
		}
		select {
		case <-timer.C:
			t.Fatal("availability timed out")
		case msg := <-o.messages:
			o.ingest(msg)
		}
	}
}

// blackhole only affects Agent MQTT; the observer and HTTP endpoint remain reachable.
func blackhole(t *testing.T, upstream string) (string, *atomic.Bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	drop := &atomic.Bool{}
	var mu sync.Mutex
	var stopped atomic.Bool
	var connections []net.Conn
	t.Cleanup(func() {
		stopped.Store(true)
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range connections {
			c.Close()
		}
	})
	pump := func(dst, src net.Conn) {
		defer dst.Close()
		defer src.Close()
		buf := make([]byte, 4096)
		for {
			n, err := src.Read(buf)
			if err != nil {
				return
			}
			if drop.Load() {
				continue
			}
			data := buf[:n]
			for len(data) > 0 {
				n, err := dst.Write(data)
				if err != nil {
					return
				}
				data = data[n:]
			}
		}
	}
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", upstream, time.Second)
			if err != nil {
				client.Close()
				continue
			}
			mu.Lock()
			if stopped.Load() {
				client.Close()
				server.Close()
				mu.Unlock()
				return
			}
			connections = append(connections, client, server)
			mu.Unlock()
			go pump(client, server)
			go pump(server, client)
		}
	}()
	return "tcp://" + listener.Addr().String(), drop
}

func logsContain(p *process, text string) bool {
	body, _ := os.ReadFile(p.log)
	return strings.Contains(string(body), text)
}
