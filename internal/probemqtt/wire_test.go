package probemqtt

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestWireClosePreventsLateConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	w := &wire{ctx: ctx, cancel: cancel}
	w.close()
	uri, _ := url.Parse("tcp://" + listener.Addr().String())
	options := mqtt.NewClientOptions().SetConnectTimeout(time.Second)
	if conn, err := w.open(uri, *options); err == nil {
		conn.Close()
		t.Fatal("canceled wire connected")
	}
}

func TestWaitCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Connect on an unused address provides a real Token without a Broker.
	options := mqtt.NewClientOptions().AddBroker("tcp://127.0.0.1:1").SetAutoReconnect(false).SetConnectRetry(false).SetConnectTimeout(100 * time.Millisecond)
	client := mqtt.NewClient(options)
	token := client.Connect()
	if err := wait(ctx, token, time.Second); err == nil {
		t.Fatal("cancellation ignored")
	}
	token.WaitTimeout(time.Second)
	client.Disconnect(0)
}
