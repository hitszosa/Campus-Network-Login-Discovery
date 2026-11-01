package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProbeDoesNotRequireBrokerCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("authenticated")) }))
	defer server.Close()
	data, err := os.ReadFile("../../configs/lab.yaml")
	if err != nil {
		t.Fatal(err)
	}
	config := strings.ReplaceAll(string(data), "http://127.0.0.1:18080/check", server.URL)
	config += "  password_env: CNLD_TEST_MISSING_FOR_PROBE\n"
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errout bytes.Buffer
	if err := execute(context.Background(), []string{"probe", "-config", path}, &out, &errout); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Labels map[string]string `json:"labels"`
		Reason string            `json:"reason"`
	}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Labels["network.campus/authenticated"] != "true" || result.Reason != "login_ok" {
		t.Fatal(out.String())
	}
	if err := execute(context.Background(), []string{"-config", path}, &out, &errout); err == nil {
		t.Fatal("agent accepted missing credential")
	}
}

func TestCommandFlags(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"-version"}} {
		if err := execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := execute(context.Background(), []string{"unexpected"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("unexpected argument accepted")
	}
}
