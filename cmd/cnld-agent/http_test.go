package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestObserve(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/target", 302)
		case "/large":
			w.Write(make([]byte, 8192))
		case "/slow":
			<-r.Context().Done()
		default:
			w.Write([]byte("authenticated"))
		}
	}))
	defer server.Close()
	client := newHTTP(100 * time.Millisecond)
	defer client.CloseIdleConnections()
	ctx := context.Background()
	if o := observe(ctx, client, server.URL); o.Body != "authenticated" {
		t.Fatal(o)
	}
	if o := observe(ctx, client, server.URL+"/redirect"); o.Code != 302 || o.Location != server.URL+"/target" {
		t.Fatal(o)
	}
	if o := observe(ctx, client, server.URL+"/large"); !o.Truncated || len(o.Body) != 4096 {
		t.Fatal("body limit")
	}
	if o := observe(ctx, client, server.URL+"/slow"); o.ErrorKind != "timeout" {
		t.Fatal(o)
	}
}

func TestObserveSlowBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client := newHTTP(100 * time.Millisecond)
	defer client.CloseIdleConnections()
	if got := observe(context.Background(), client, server.URL); got.ErrorKind != "timeout" {
		t.Fatalf("slow response body classified as %q", got.ErrorKind)
	}
}
