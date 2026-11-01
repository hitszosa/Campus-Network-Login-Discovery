package probecore

import (
	"net/url"
	"strings"
	"time"
)

func Classify(o Observation, rules Rules, at time.Time) Result {
	r := Result{Authenticated: AuthUnknown, ExternalAccess: "unknown",
		Reason: "unrecognized_response", ObservedAt: at, Duration: o.Duration}
	switch {
	case o.ErrorKind != "":
		r.Reason = o.ErrorKind
	case o.Truncated:
		r.Reason = "body_too_large"
	case matches(rules.Authenticated, o):
		r.Authenticated, r.Reason = AuthYes, "login_ok"
	case matches(rules.Unauthenticated, o):
		r.Authenticated, r.Reason = AuthNo, "explicit_unauthenticated"
	default:
		u, err := url.Parse(o.Location)
		if err == nil && (o.Code == 301 || o.Code == 302 || o.Code == 303 || o.Code == 307 || o.Code == 308) {
			for _, host := range rules.PortalHosts {
				if (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Hostname(), host) {
					r.Authenticated, r.Reason = AuthNo, "known_portal"
				}
			}
		}
	}
	return r
}
