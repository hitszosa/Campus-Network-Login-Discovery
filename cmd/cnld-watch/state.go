package main

import (
	"fmt"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

type view struct {
	availability probemqtt.Availability
	reports      map[string]probemqtt.Report
}

func newView() *view { return &view{reports: make(map[string]probemqtt.Report)} }

func (v *view) accept(r probemqtt.Report) {
	old, ok := v.reports[r.ConnectionID]
	if ok && r.Sequence <= old.Sequence {
		return
	}
	if !ok && len(v.reports) >= 2 {
		v.reports = make(map[string]probemqtt.Report)
	}
	v.reports[r.ConnectionID] = r
}

func (v *view) current(connected bool, node string, now time.Time) string {
	a := v.availability
	if !connected || a.NodeID != node || a.SchemaVersion != 1 || a.ConnectionID == "" {
		return "unknown"
	}
	if a.Status == "offline" {
		return "offline"
	}
	if a.Status != "online" {
		return "unknown"
	}
	r, ok := v.reports[a.ConnectionID]
	if !ok || r.NodeID != node || r.SchemaVersion != 1 || r.Sequence == 0 || r.ValidForSeconds <= 0 ||
		r.ValidForSeconds > 86400 || r.ObservedAt.After(now) ||
		!now.Before(r.ObservedAt.Add(time.Duration(r.ValidForSeconds)*time.Second)) {
		return "unknown"
	}
	auth, ext := r.Labels["network.campus/authenticated"], r.Labels["network.campus/external_access"]
	if auth != "true" && auth != "false" && auth != "unknown" {
		return "unknown"
	}
	if ext != "reachable" && ext != "blocked" && ext != "unknown" {
		return "unknown"
	}
	return fmt.Sprintf("auth=%s external=%s reason=%s", auth, ext, r.Reason)
}
