package probecore

import (
	"fmt"
	"net/url"
)

func (r Rules) Validate() error {
	a, b := r.Authenticated, r.Unauthenticated
	for _, rule := range []Rule{a, b} {
		if rule.StatusCode < 100 || rule.StatusCode > 599 {
			return fmt.Errorf("status_code must be in 100..599")
		}
	}
	if a.StatusCode == b.StatusCode &&
		(a.BodyEquals == nil || b.BodyEquals == nil || *a.BodyEquals == *b.BodyEquals) {
		return fmt.Errorf("authentication rules overlap")
	}
	if len(r.PortalHosts) > 0 && a.StatusCode >= 300 && a.StatusCode < 400 {
		return fmt.Errorf("success redirect conflicts with portal rules")
	}
	for _, host := range r.PortalHosts {
		u, err := url.Parse("http://" + host)
		if err != nil || u.Hostname() == "" || u.Host != host || u.Path != "" || u.Port() != "" {
			return fmt.Errorf("portal_hosts expects hostnames without scheme or port")
		}
	}
	return nil
}

func matches(rule Rule, o Observation) bool {
	return o.Code == rule.StatusCode && (rule.BodyEquals == nil || *rule.BodyEquals == o.Body)
}
