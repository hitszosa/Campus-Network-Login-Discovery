package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

type certificates struct{ ca, server, serverKey, client, clientKey string }

func createCertificates(t *testing.T) certificates {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "CNLD test CA"},
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, block *pem.Block) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	files := certificates{ca: write("ca.crt", &pem.Block{Type: "CERTIFICATE", Bytes: der})}
	leaf := func(name string, usage x509.ExtKeyUsage, id int64) (string, string) {
		pair, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(id), Subject: pkix.Name{CommonName: name},
			NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &pair.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := x509.MarshalPKCS8PrivateKey(pair)
		if err != nil {
			t.Fatal(err)
		}
		return write(name+".crt", &pem.Block{Type: "CERTIFICATE", Bytes: der}), write(name+".key", &pem.Block{Type: "PRIVATE KEY", Bytes: raw})
	}
	files.server, files.serverKey = leaf("server", x509.ExtKeyUsageServerAuth, 2)
	files.client, files.clientKey = leaf("client", x509.ExtKeyUsageClientAuth, 3)
	return files
}

func TestTLSACLAndPersistence(t *testing.T) {
	requireIntegration(t)
	certs := createCertificates(t)
	dir := t.TempDir()
	passwordFile := filepath.Join(dir, "passwords")
	for i, user := range []string{"agent-lab", "observer"} {
		args := []string{"-b"}
		if i == 0 {
			args = append(args, "-c")
		}
		args = append(args, passwordFile, user, user+"-test-only")
		if out, err := exec.Command("mosquitto_passwd", args...).CombinedOutput(); err != nil {
			t.Fatalf("password file: %s %v", out, err)
		}
	}
	acl := filepath.Join(dir, "acl")
	if err := os.WriteFile(acl, []byte("user agent-lab\ntopic write cnld/nodes/lab-01/report\ntopic write cnld/nodes/lab-01/availability\nuser observer\ntopic read cnld/nodes/+/report\ntopic read cnld/nodes/+/availability\n"), 0600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	extra := fmt.Sprintf("allow_anonymous false\npassword_file %s\nacl_file %s\ncafile %s\ncertfile %s\nkeyfile %s\nrequire_certificate true\npersistence true\npersistence_location %s/\n", passwordFile, acl, certs.ca, certs.server, certs.serverKey, dataDir)
	l := newLab(t, extra)
	broker := "tls://" + l.address
	clientTLS, err := probemqtt.TLSConfig(probemqtt.Config{CAFile: certs.ca, CertFile: certs.client, KeyFile: certs.clientKey})
	if err != nil {
		t.Fatal(err)
	}
	observer := observe(t, broker, clientTLS, "observer", "observer-test-only")
	agentOptions := fmt.Sprintf("  ca_file: %s\n  cert_file: %s\n  key_file: %s\n  username_env: CNLD_TEST_USER\n  password_env: CNLD_TEST_PASSWORD\n", certs.ca, certs.client, certs.clientKey)
	credentials := []string{"CNLD_TEST_USER=agent-lab", "CNLD_TEST_PASSWORD=agent-lab-test-only"}
	agent := l.agent(broker, agentOptions, credentials...)
	report := observer.report(t, func(r probemqtt.Report) bool { return r.Reason == "login_ok" })
	watcher := start(t, []string{"CNLD_MQTT_USERNAME=observer", "CNLD_MQTT_PASSWORD=observer-test-only"}, watchBinary,
		"-broker", broker, "-ca-file", certs.ca, "-cert-file", certs.client, "-key-file", certs.clientKey)
	eventually(t, func() bool { return logsContain(watcher, "auth=true") })
	agent.stop(syscall.SIGTERM)
	observer.availability(t, "offline", "graceful_shutdown", report.ConnectionID)
	l.broker.stop(syscall.SIGTERM)
	l.restartBroker()
	observer = observe(t, broker, clientTLS, "observer", "observer-test-only")
	observer.report(t, func(r probemqtt.Report) bool { return r.ConnectionID == report.ConnectionID })
	observer.availability(t, "offline", "graceful_shutdown", report.ConnectionID)
	eventually(t, func() bool { return logsContain(watcher, "offline") })

	wrongCA := createCertificates(t)
	bad := l.agent(broker, strings.ReplaceAll(agentOptions, certs.ca, wrongCA.ca), credentials...)
	eventually(t, func() bool { return logsContain(bad, "certificate") })
	if logsContain(bad, "mqtt online") {
		t.Fatal("untrusted CA accepted")
	}
	bad.stop(syscall.SIGTERM)
	missingCert := fmt.Sprintf("  ca_file: %s\n  username_env: CNLD_TEST_USER\n  password_env: CNLD_TEST_PASSWORD\n", certs.ca)
	bad = l.agent(broker, missingCert, credentials...)
	eventually(t, func() bool { return logsContain(bad, "mqtt retry") })
	if logsContain(bad, "mqtt online") {
		t.Fatal("Broker accepted missing client certificate")
	}
	bad.stop(syscall.SIGTERM)

	// The observer may read all reports, but may not write any report.
	token := observer.client.Publish("cnld/nodes/other/report", 1, true, []byte("forbidden"))
	token.WaitTimeout(time.Second)
	eventually(t, func() bool {
		body, _ := os.ReadFile(l.broker.log)
		return strings.Contains(strings.ToLower(string(body)), "denied publish")
	})
	fresh := observe(t, broker, clientTLS, "observer", "observer-test-only")
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case message := <-fresh.messages:
			if message.topic == "cnld/nodes/other/report" {
				t.Fatal("ACL allowed unauthorized publish")
			}
		}
	}
}
