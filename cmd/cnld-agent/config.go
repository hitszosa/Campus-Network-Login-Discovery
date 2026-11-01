package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"
	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
	"go.yaml.in/yaml/v3"
)

type ProbeConfig struct {
	Endpoint string          `yaml:"endpoint"`
	Interval time.Duration   `yaml:"interval"`
	Timeout  time.Duration   `yaml:"timeout"`
	ValidFor time.Duration   `yaml:"valid_for"`
	Rules    probecore.Rules `yaml:",inline"`
}

type Config struct {
	NodeID string           `yaml:"node_id"`
	Sinks  []string         `yaml:"sinks"`
	Probe  ProbeConfig      `yaml:"probe"`
	MQTT   probemqtt.Config `yaml:"mqtt"`
}

func readConfig(path string) (Config, error) {
	var c Config
	file, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("config must contain exactly one YAML document")
	}
	if c.MQTT.TopicPrefix == "" {
		c.MQTT.TopicPrefix = "cnld/nodes"
	}
	if c.MQTT.ClientID == "" {
		c.MQTT.ClientID = "cnld-" + c.NodeID
	}
	if c.MQTT.KeepAlive == 0 {
		c.MQTT.KeepAlive = 30 * time.Second
	}
	if c.MQTT.PublishTimeout == 0 {
		c.MQTT.PublishTimeout = 5 * time.Second
	}
	if c.MQTT.MaxBackoff == 0 {
		c.MQTT.MaxBackoff = 30 * time.Second
	}
	return c, nil
}

func loadConfig(path string) (Config, error) {
	c, err := readConfig(path)
	if err != nil {
		return c, err
	}
	return c, c.validate()
}
