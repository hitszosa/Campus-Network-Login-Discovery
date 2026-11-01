package main

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (c Config) validate() error {
	if !regexp.MustCompile(`^[A-Za-z0-9._-]+$`).MatchString(c.NodeID) {
		return fmt.Errorf("invalid node_id")
	}
	if len(c.Sinks) != 1 || c.Sinks[0] != "mqtt" {
		return fmt.Errorf("this build supports sinks: [mqtt] only")
	}
	if err := c.Probe.validate(); err != nil {
		return err
	}
	b, err := url.Parse(c.MQTT.Broker)
	if err != nil || b.Hostname() == "" || b.Port() == "" || b.User != nil || b.Path != "" || b.RawQuery != "" || b.Fragment != "" ||
		(b.Scheme != "tcp" && b.Scheme != "tls" && b.Scheme != "ssl") {
		return fmt.Errorf("mqtt.broker must be tcp/tls/ssl://host:port")
	}
	port, err := strconv.Atoi(b.Port())
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid MQTT port")
	}
	m := c.MQTT
	if m.TopicPrefix == "" || strings.ContainsAny(m.TopicPrefix, "+#\x00") || strings.HasSuffix(m.TopicPrefix, "/") {
		return fmt.Errorf("invalid topic_prefix")
	}
	if m.KeepAlive < time.Second || m.KeepAlive > 65535*time.Second || m.KeepAlive%time.Second != 0 ||
		m.PublishTimeout <= 0 || m.MaxBackoff < time.Second || m.MaxBackoff > 24*time.Hour {
		return fmt.Errorf("invalid MQTT timing")
	}
	for _, name := range []string{m.UsernameEnv, m.PasswordEnv} {
		if name != "" {
			if _, ok := os.LookupEnv(name); !ok {
				return fmt.Errorf("missing environment variable %s", name)
			}
		}
	}
	return nil
}

func (p ProbeConfig) validate() error {
	u, err := url.Parse(p.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("probe.endpoint must be an http(s) URL without credentials")
	}
	if p.Timeout <= 0 || p.Interval <= p.Timeout || p.ValidFor <= p.Interval || p.ValidFor-p.Interval <= p.Timeout || p.ValidFor > 24*time.Hour || p.ValidFor%time.Second != 0 {
		return fmt.Errorf("need 0 < timeout < interval; valid_for > interval + timeout, in whole seconds")
	}
	if err := p.Rules.Validate(); err != nil {
		return err
	}

	return nil
}
