package probemqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func TLSConfig(c Config) (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA file has no certificates")
		}
		t.RootCAs = roots
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, fmt.Errorf("cert_file and key_file must be paired")
	}
	if c.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		t.Certificates = []tls.Certificate{pair}
	}
	return t, nil
}
