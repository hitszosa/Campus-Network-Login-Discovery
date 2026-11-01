package probemqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// 保存底层 socket；Close 不发送 MQTT DISCONNECT。
type wire struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func (w *wire) open(uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(w.ctx, options.ConnectTimeout)
	defer cancel()
	d := &net.Dialer{}
	var conn net.Conn
	var err error
	switch uri.Scheme {
	case "tcp":
		conn, err = d.DialContext(ctx, "tcp", uri.Host)
	case "tls", "ssl":
		td := &tls.Dialer{NetDialer: d, Config: options.TLSConfig}
		conn, err = td.DialContext(ctx, "tcp", uri.Host)
	default:
		return nil, fmt.Errorf("unsupported broker scheme")
	}
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		conn.Close()
		return nil, fmt.Errorf("connection attempt canceled")
	}
	w.conn = conn
	return conn, nil
}

func (w *wire) close() {
	w.cancel()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.conn != nil {
		w.conn.Close()
	}
}

func wait(ctx context.Context, token mqtt.Token, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("mqtt operation timed out")
	case <-token.Done():
		return token.Error()
	}
}
