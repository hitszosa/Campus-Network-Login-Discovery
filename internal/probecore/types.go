package probecore

import "time"

type Auth string

const (
	AuthYes     Auth = "true"
	AuthNo      Auth = "false"
	AuthUnknown Auth = "unknown"
)

type Observation struct {
	Code      int
	Body      string
	Location  string
	Truncated bool
	ErrorKind string
	Duration  time.Duration
}

type Rule struct {
	StatusCode int     `yaml:"status_code"`
	BodyEquals *string `yaml:"body_equals"`
}

type Rules struct {
	Authenticated   Rule     `yaml:"authenticated_rule"`
	Unauthenticated Rule     `yaml:"unauthenticated_rule"`
	PortalHosts     []string `yaml:"portal_hosts"`
}

type Result struct {
	Authenticated  Auth
	ExternalAccess string
	Reason         string
	ObservedAt     time.Time
	Duration       time.Duration
}

func (r Result) Labels() map[string]string {
	return map[string]string{
		"network.campus/authenticated":   string(r.Authenticated),
		"network.campus/external_access": r.ExternalAccess,
	}
}
