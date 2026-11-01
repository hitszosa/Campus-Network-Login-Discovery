package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

type event struct {
	topic string
	body  []byte
}

func main() {
	node := flag.String("node", "lab-01", "node ID")
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "lab broker")
	prefix := flag.String("prefix", "cnld/nodes", "topic prefix")
	ca := flag.String("ca-file", "", "custom CA PEM")
	cert := flag.String("cert-file", "", "client certificate PEM")
	key := flag.String("key-file", "", "client key PEM")
	usernameEnv := flag.String("username-env", "CNLD_MQTT_USERNAME", "username environment variable")
	passwordEnv := flag.String("password-env", "CNLD_MQTT_PASSWORD", "password environment variable")
	flag.Parse()
	tlsConfig, err := probemqtt.TLSConfig(probemqtt.Config{CAFile: *ca, CertFile: *cert, KeyFile: *key})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 每次订阅重连都重新建立视图。
	for ctx.Err() == nil {
		watch(ctx, *broker, *prefix, *node, tlsConfig, os.Getenv(*usernameEnv), os.Getenv(*passwordEnv))
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func watch(ctx context.Context, broker, prefix, node string, tlsConfig *tls.Config, username, password string) {
	events := make(chan event, 16)
	lost := make(chan struct{}, 1)
	notifyLost := func() {
		select {
		case lost <- struct{}{}:
		default:
		}
	}
	options := mqtt.NewClientOptions().AddBroker(broker).
		SetClientID(fmt.Sprintf("cnld-watch-%d-%d", os.Getpid(), time.Now().UnixNano())).SetProtocolVersion(4).
		SetCleanSession(true).SetAutoReconnect(false).SetConnectRetry(false).
		SetConnectTimeout(2 * time.Second).SetKeepAlive(4 * time.Second).SetPingTimeout(time.Second).
		SetTLSConfig(tlsConfig).SetUsername(username).SetPassword(password)
	options.SetConnectionLostHandler(func(mqtt.Client, error) { notifyLost() })
	client := mqtt.NewClient(options)
	defer client.Disconnect(100)
	token := client.Connect()
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		log.Print("unknown: connect failed")
		return
	}
	topic := prefix + "/" + node + "/"
	handler := func(_ mqtt.Client, m mqtt.Message) {
		e := event{m.Topic(), append([]byte(nil), m.Payload()...)}
		select {
		case events <- e:
		default:
			notifyLost()
		} // 队列满则重新订阅，重新建立可信状态
	}
	token = client.SubscribeMultiple(map[string]byte{topic + "report": 1, topic + "availability": 1}, handler)
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		log.Print("unknown: subscribe failed")
		return
	}
	state := newView() // 每次重连都清空本地视图
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	previous := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-lost:
			log.Print("unknown: subscription lost")
			return
		case e := <-events:
			if strings.HasSuffix(e.topic, "/report") {
				var r probemqtt.Report
				if json.Unmarshal(e.body, &r) == nil {
					state.accept(r)
				}
			} else {
				var a probemqtt.Availability
				if json.Unmarshal(e.body, &a) == nil {
					state.availability = a
				}
			}
		case <-ticker.C:
			value := state.current(client.IsConnectionOpen(), node, time.Now())
			if value != previous {
				log.Print(value)
				previous = value
			}
		}
	}
}
