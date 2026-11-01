package main

import (
	"context"
	"log"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"
)

type sink interface{ Submit(probecore.Result) }

func runProbe(ctx context.Context, interval time.Duration, trigger <-chan struct{},
	sample func(context.Context) probecore.Result, outputs ...sink) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	previous := probecore.Auth("")
	for {
		if ctx.Err() != nil {
			return
		}
		result := sample(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("probe auth=%s reason=%s changed=%t", result.Authenticated, result.Reason, result.Authenticated != previous)
		previous = result.Authenticated
		for _, output := range outputs {
			output.Submit(result)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-trigger:
		}
	}
}
