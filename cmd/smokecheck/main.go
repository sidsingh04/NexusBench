// cmd/smokecheck/main.go
//
// A minimal CLI tool used exclusively by the smoke test to verify that the
// jobs.benchmark topic has received at least one message since a given offset.
//
// Usage:
//
//	go run ./cmd/smokecheck watermark          # print total high-watermark
//	go run ./cmd/smokecheck verify <before>    # exit 0 if watermark > before
//
// Configuration:
//
//	REDPANDA_BROKERS — read via config.Load(), same as the control plane.
//	                   Default: 127.0.0.1:19092
//
// Uses franz-go kadm.ListEndOffsets — the same client and library version as
// the production queue package, so broker compatibility is guaranteed.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/queue"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: smokecheck <watermark|verify> [before]")
		os.Exit(2)
	}

	// config.Load() owns all broker address parsing — no duplication.
	brokers := config.Load().RedpandaBrokers

	total, err := sumWatermark(brokers, queue.TopicJobs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "watermark":
		fmt.Println(total)

	case "verify":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: smokecheck verify <before>")
			os.Exit(2)
		}
		before, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid before value %q: %v\n", os.Args[2], err)
			os.Exit(2)
		}
		delta := total - before
		fmt.Printf("before=%d after=%d delta=%d\n", before, total, delta)
		if delta > 0 {
			os.Exit(0)
		}
		os.Exit(1)

	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// sumWatermark returns the sum of end offsets (high-watermarks) across all
// partitions of the given topic. An end offset of N means N messages have
// been durably written to that partition since topic creation.
func sumWatermark(brokers []string, topic string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, fmt.Errorf("connect to %v: %w", brokers, err)
	}
	defer cl.Close()

	offsets, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("ListEndOffsets(%q): %w", topic, err)
	}

	var total int64
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			total += o.Offset
		}
	})
	return total, nil
}
