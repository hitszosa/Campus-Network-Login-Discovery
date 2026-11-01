package probecore

import (
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	yes, no := "authenticated", "unauthenticated"
	rules := Rules{Authenticated: Rule{200, &yes}, Unauthenticated: Rule{200, &no},
		PortalHosts: []string{"login.example.edu"}}
	if err := rules.Validate(); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		in   Observation
		want Auth
	}{
		{"yes", Observation{Code: 200, Body: yes}, AuthYes},
		{"no", Observation{Code: 200, Body: no}, AuthNo},
		{"portal", Observation{Code: 302, Location: "https://login.example.edu/"}, AuthNo},
		{"other redirect", Observation{Code: 302, Location: "https://elsewhere.example/"}, AuthUnknown},
		{"timeout", Observation{ErrorKind: "timeout"}, AuthUnknown},
		{"dns", Observation{ErrorKind: "dns_error"}, AuthUnknown},
		{"connection", Observation{ErrorKind: "connection_error"}, AuthUnknown},
		{"unexpected", Observation{Code: 200, Body: "hello"}, AuthUnknown},
		{"truncated", Observation{Code: 200, Body: yes, Truncated: true}, AuthUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.in, rules, at)
			if got.Authenticated != tc.want || !got.ObservedAt.Equal(at) || got.ExternalAccess != "unknown" {
				t.Fatalf("got %+v; want auth=%s", got, tc.want)
			}
		})
	}
	rules.Unauthenticated = rules.Authenticated
	if rules.Validate() == nil {
		t.Fatal("overlapping rules accepted")
	}
}
