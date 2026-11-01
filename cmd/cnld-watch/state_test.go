package main

import (
	"testing"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

func TestView(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	report := probemqtt.Report{SchemaVersion: 1, NodeID: "lab-01", ConnectionID: "new", Sequence: 2,
		ObservedAt: now, ValidForSeconds: 10, Labels: map[string]string{
			"network.campus/authenticated": "true", "network.campus/external_access": "unknown"}, Reason: "login_ok"}
	online := probemqtt.Availability{SchemaVersion: 1, NodeID: "lab-01", ConnectionID: "new", Status: "online"}
	for _, reportFirst := range []bool{true, false} {
		v := newView()
		if reportFirst {
			v.accept(report)
		} else {
			v.availability = online
		}
		if v.current(true, "lab-01", now) != "unknown" {
			t.Fatal("incomplete state")
		}
		v.accept(report)
		v.availability = online
		want := "auth=true external=unknown reason=login_ok"
		if got := v.current(true, "lab-01", now); got != want {
			t.Fatal(got)
		}
		duplicate := report
		duplicate.Sequence = 1
		duplicate.Reason = "old"
		v.accept(duplicate)
		v.accept(report)
		if v.current(true, "lab-01", now) != want {
			t.Fatal("duplicate or old sequence replaced latest")
		}
		if v.current(true, "lab-01", now.Add(10*time.Second)) != "unknown" {
			t.Fatal("TTL")
		}
		if v.current(false, "lab-01", now) != "unknown" {
			t.Fatal("consumer disconnected")
		}
		v.availability.ConnectionID = "other"
		if v.current(true, "lab-01", now) != "unknown" {
			t.Fatal("ID mismatch")
		}
		v.availability = online
		v.availability.Status = "offline"
		if v.current(true, "lab-01", now) != "offline" {
			t.Fatal("offline")
		}
	}
}

func TestViewRejectsInvalidAndFutureReport(t *testing.T) {
	now := time.Now()
	valid := probemqtt.Report{SchemaVersion: 1, NodeID: "lab-01", ConnectionID: "conn", Sequence: 1,
		ObservedAt: now, ValidForSeconds: 10, Labels: map[string]string{"network.campus/authenticated": "true", "network.campus/external_access": "unknown"}}
	for _, change := range []func(*probemqtt.Report){
		func(r *probemqtt.Report) { r.ObservedAt = now.Add(time.Hour) },
		func(r *probemqtt.Report) { r.SchemaVersion = 2 },
		func(r *probemqtt.Report) { r.NodeID = "other" },
		func(r *probemqtt.Report) { r.Sequence = 0 },
		func(r *probemqtt.Report) { r.ValidForSeconds = 0 },
		func(r *probemqtt.Report) { r.Labels = nil },
	} {
		v := newView()
		r := valid
		change(&r)
		v.accept(r)
		v.availability = probemqtt.Availability{SchemaVersion: 1, NodeID: "lab-01", ConnectionID: "conn", Status: "online"}
		if got := v.current(true, "lab-01", now); got != "unknown" {
			t.Fatal(got)
		}
	}
}
