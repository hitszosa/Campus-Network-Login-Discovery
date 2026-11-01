package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probecore"
	"github.com/hitszosa/Campus-Network-Login-Discovery/internal/probemqtt"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, out, errout io.Writer) error {
	once := len(args) > 0 && args[0] == "probe"
	if once {
		args = args[1:]
	}
	flags := flag.NewFlagSet("cnld-agent", flag.ContinueOnError)
	flags.SetOutput(errout)
	path := flags.String("config", "configs/lab.yaml", "YAML configuration")
	showVersion := flags.Bool("version", false, "print build version")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *showVersion {
		_, err := fmt.Fprintln(out, version)
		return err
	}
	c, err := readConfig(*path)
	if err != nil {
		return err
	}
	if once {
		err = c.Probe.validate()
	} else {
		err = c.validate()
	}
	if err != nil {
		return err
	}
	client := newHTTP(c.Probe.Timeout)
	defer client.CloseIdleConnections()
	sample := func(ctx context.Context) probecore.Result {
		at := time.Now()
		probeCtx, cancel := context.WithTimeout(ctx, c.Probe.Timeout)
		defer cancel()
		return probecore.Classify(observe(probeCtx, client, c.Probe.Endpoint), c.Probe.Rules, at)
	}
	if once {
		r := sample(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return json.NewEncoder(out).Encode(struct {
			ObservedAt time.Time         `json:"observed_at"`
			Labels     map[string]string `json:"labels"`
			Reason     string            `json:"reason"`
			DurationMS int64             `json:"probe_duration_ms"`
		}{r.ObservedAt.UTC(), r.Labels(), r.Reason, r.Duration.Milliseconds()})
	}
	tlsConfig, err := probemqtt.TLSConfig(c.MQTT)
	if err != nil {
		return err
	}
	log.SetPrefix("node=" + c.NodeID + " ")
	trigger := make(chan struct{}, 1)
	worker := &probemqtt.Worker{Mailbox: probemqtt.NewMailbox(), Config: c.MQTT,
		Node: c.NodeID, ValidFor: c.Probe.ValidFor, TLS: tlsConfig, Trigger: trigger}
	done := make(chan struct{})
	go func() { defer close(done); worker.Run(ctx) }()
	runProbe(ctx, c.Probe.Interval, trigger, sample, worker)
	<-done
	return nil
}
