package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"
)

func newHTTP(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // 直接测本机出口
	transport.DialContext = (&net.Dialer{Timeout: timeout}).DialContext
	transport.ResponseHeaderTimeout = timeout
	transport.TLSHandshakeTimeout = timeout
	return &http.Client{Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func observe(ctx context.Context, client *http.Client, endpoint string) probecore.Observation {
	start := time.Now()
	o := probecore.Observation{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		o.ErrorKind = "request_error"
		return o
	}
	resp, err := client.Do(req)
	if err != nil {
		o.ErrorKind = errorKind(err)
	} else {
		defer resp.Body.Close()
		o.Code = resp.StatusCode
		if location, e := resp.Location(); e == nil {
			o.Location = location.String()
		}
		const limit = 4096
		body, e := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		o.Truncated = len(body) > limit
		if o.Truncated {
			body = body[:limit]
		}
		o.Body = string(body)
		if e != nil {
			o.ErrorKind = errorKind(e)
		}
	}
	o.Duration = time.Since(start)
	return o
}

func errorKind(err error) string {
	var dns *net.DNSError
	var ne net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case errors.As(err, &dns):
		return "dns_error"
	default:
		return "connection_error"
	}
}
