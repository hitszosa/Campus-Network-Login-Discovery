package main

import (
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	c, err := loadConfig("../../configs/lab.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c.Probe.Timeout = 5 * time.Second
	if c.validate() == nil {
		t.Fatal("timeout >= interval accepted")
	}
}
