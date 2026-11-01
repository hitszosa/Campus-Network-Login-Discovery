package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInvalidConfiguration(t *testing.T) {
	base, err := loadConfig("../../configs/lab.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"node wildcard", func(c *Config) { c.NodeID = "node/+" }},
		{"unsupported K8s", func(c *Config) { c.Sinks = []string{"k8s"} }},
		{"URL credentials", func(c *Config) { c.Probe.Endpoint = "http://secret:password@example.edu" }},
		{"short TTL", func(c *Config) { c.Probe.ValidFor = 4 * time.Second }},
		{"fractional TTL", func(c *Config) { c.Probe.ValidFor = 10500 * time.Millisecond }},
		{"large TTL", func(c *Config) { c.Probe.ValidFor = 25 * time.Hour }},
		{"overlapping rules", func(c *Config) { c.Probe.Rules.Unauthenticated = c.Probe.Rules.Authenticated }},
		{"broker password", func(c *Config) { c.MQTT.Broker = "tcp://secret:password@localhost:1883" }},
		{"bad port", func(c *Config) { c.MQTT.Broker = "tcp://localhost:65536" }},
		{"wildcard topic", func(c *Config) { c.MQTT.TopicPrefix = "nodes/+" }},
		{"zero keep alive", func(c *Config) { c.MQTT.KeepAlive = 0 }},
		{"fractional keep alive", func(c *Config) { c.MQTT.KeepAlive = 1500 * time.Millisecond }},
		{"negative publish timeout", func(c *Config) { c.MQTT.PublishTimeout = -time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			if c.validate() == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestReadConfigDocuments(t *testing.T) {
	data, err := os.ReadFile("../../configs/lab.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{string(data) + "\nunknown_option: true\n", string(data) + "\n---\nnode_id: other\n"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readConfig(path); err == nil {
			t.Fatal("invalid document accepted")
		}
	}
}

func TestMissingCredentialDoesNotLeakSecret(t *testing.T) {
	c, err := loadConfig("../../configs/lab.yaml")
	if err != nil {
		t.Fatal(err)
	}
	name := "CNLD_TEST_ABSENT_CREDENTIAL"
	t.Setenv(name, "")
	os.Unsetenv(name)
	c.MQTT.PasswordEnv = name
	err = c.validate()
	if err == nil || !strings.Contains(err.Error(), name) {
		t.Fatal(err)
	}
}
