package probemqtt

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type session struct {
	client   mqtt.Client
	wire     *wire
	lost     chan struct{}
	id       string
	readyAt  time.Time
	sequence uint64
}

func newSession(ctx context.Context, c Config, node string, tlsConfig *tls.Config) (*session, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	dialCtx, cancel := context.WithCancel(ctx)
	s := &session{wire: &wire{ctx: dialCtx, cancel: cancel}, lost: make(chan struct{}, 1), id: hex.EncodeToString(id[:])}
	will, err := json.Marshal(Availability{SchemaVersion: 1, NodeID: node, ConnectionID: s.id, Status: "offline", Reason: "connection_lost"})
	if err != nil {
		return nil, err
	}
	options := mqtt.NewClientOptions().AddBroker(c.Broker).SetClientID(c.ClientID).
		SetProtocolVersion(4).SetCleanSession(true).SetAutoReconnect(false).SetConnectRetry(false).
		SetKeepAlive(c.KeepAlive).SetPingTimeout(c.PublishTimeout).
		SetConnectTimeout(c.PublishTimeout).SetWriteTimeout(c.PublishTimeout).
		SetTLSConfig(tlsConfig).SetCustomOpenConnectionFn(s.wire.open).
		SetUsername(os.Getenv(c.UsernameEnv)).SetPassword(os.Getenv(c.PasswordEnv)).
		SetBinaryWill(c.TopicPrefix+"/"+node+"/availability", will, 1, true)
	options.SetConnectionLostHandler(func(mqtt.Client, error) {
		select {
		case s.lost <- struct{}{}:
		default:
		}
	})
	s.client = mqtt.NewClient(options)
	return s, nil
}

func (s *session) publish(ctx context.Context, c Config, topic string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return wait(ctx, s.client.Publish(topic, 1, true, body), c.PublishTimeout)
}

func (s *session) availability(ctx context.Context, c Config, node, status, reason string) error {
	return s.publish(ctx, c, c.TopicPrefix+"/"+node+"/availability",
		Availability{1, node, s.id, status, reason})
}

func (s *session) abort() {
	s.wire.close()         // 顺序关键：先关闭 socket，Broker 才有机会发布遗嘱
	s.client.Disconnect(0) // 只清理库的资源；socket 已关，不能再发送 DISCONNECT
}
