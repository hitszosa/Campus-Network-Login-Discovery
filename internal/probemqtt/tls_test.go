package probemqtt

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
)

func TestTLSConfig(t *testing.T) {
	config, err := TLSConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if config.InsecureSkipVerify || config.MinVersion < tls.VersionTLS12 {
		t.Fatal("TLS verification disabled")
	}
	if _, err := TLSConfig(Config{CertFile: "unpaired.crt"}); err == nil {
		t.Fatal("unpaired certificate accepted")
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := TLSConfig(Config{CAFile: path}); err == nil {
		t.Fatal("invalid CA accepted")
	}
}
