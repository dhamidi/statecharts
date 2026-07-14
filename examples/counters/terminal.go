package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const terminalRefresh = 500 * time.Millisecond

type colorWriteStats struct {
	Color     string
	Generated uint64
	Attempts  uint64
	Succeeded uint64
	Retries   uint64
	InFlight  uint64
}

type writerStats struct {
	mu      sync.Mutex
	byColor map[string]*colorWriteStats
}

func newWriterStats(selected []string) *writerStats {
	s := &writerStats{byColor: make(map[string]*colorWriteStats, len(selected))}
	for _, color := range selected {
		s.byColor[color] = &colorWriteStats{Color: color}
	}
	return s
}

func (s *writerStats) generated(color string) {
	s.mu.Lock()
	s.byColor[color].Generated++
	s.byColor[color].InFlight++
	s.mu.Unlock()
}

func (s *writerStats) attempted(color string) {
	s.mu.Lock()
	s.byColor[color].Attempts++
	s.mu.Unlock()
}

func (s *writerStats) succeeded(color string) {
	s.mu.Lock()
	s.byColor[color].Succeeded++
	s.mu.Unlock()
}

func (s *writerStats) retried(color string) {
	s.mu.Lock()
	s.byColor[color].Retries++
	s.mu.Unlock()
}

func (s *writerStats) completed(color string) {
	s.mu.Lock()
	s.byColor[color].InFlight--
	s.mu.Unlock()
}

func (s *writerStats) snapshot(selected []string) []colorWriteStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]colorWriteStats, 0, len(selected))
	for _, color := range selected {
		out = append(out, *s.byColor[color])
	}
	return out
}

func runWriterTerminal(ctx context.Context, connection *connectionActor, selected []string, targetRate float64, stats *writerStats) {
	ticker := time.NewTicker(terminalRefresh)
	defer ticker.Stop()
	previous := make(map[string]uint64, len(selected))
	history := make(map[string][]uint64, len(selected))
	draw := func() {
		frame := writerTerminalFrame(connection.status(), targetRate, stats.snapshot(selected), previous, history)
		fmt.Fprint(os.Stdout, "\x1b[H\x1b[2J", frame)
	}
	draw()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stdout)
			return
		case <-ticker.C:
			draw()
		}
	}
}

func writerTerminalFrame(status string, targetRate float64, snapshots []colorWriteStats, previous map[string]uint64, history map[string][]uint64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "COUNTER WRITER / LOAD GENERATOR\nconnection: %-12s target: %.1f writes/s\n\n", status, targetRate)
	fmt.Fprintln(&b, "color    offered   writes  attempts   success   retries  in-flight  recent offered load")
	for _, s := range snapshots {
		delta := s.Generated - previous[s.Color]
		previous[s.Color] = s.Generated
		h := append(history[s.Color], delta)
		if len(h) > 24 {
			h = h[len(h)-24:]
		}
		history[s.Color] = h
		rate := float64(delta) / terminalRefresh.Seconds()
		fmt.Fprintf(&b, "%-8s %7.1f/s %8d %9d %9d %9d %10d  %s\n",
			s.Color, rate, s.Generated, s.Attempts, s.Succeeded, s.Retries, s.InFlight, sparkline(h, 24))
	}
	return b.String()
}

func sparkline(samples []uint64, width int) string {
	if width <= 0 {
		return ""
	}
	if len(samples) > width {
		samples = samples[len(samples)-width:]
	}
	var max uint64
	for _, sample := range samples {
		if sample > max {
			max = sample
		}
	}
	blocks := []rune("▁▂▃▄▅▆▇█")
	out := make([]rune, width)
	padding := width - len(samples)
	for i := range padding {
		out[i] = blocks[0]
	}
	for i, sample := range samples {
		level := 0
		if max > 0 {
			level = int(sample * uint64(len(blocks)-1) / max)
		}
		out[padding+i] = blocks[level]
	}
	return string(out)
}

func runReaderTerminal(ctx context.Context, connection *connectionActor, selected []string, state *readerState) {
	ticker := time.NewTicker(terminalRefresh)
	defer ticker.Stop()
	draw := func() {
		fmt.Fprint(os.Stdout, "\x1b[H\x1b[2J", readerTerminalFrame(connection.status(), selected, state.snapshot()))
	}
	draw()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stdout)
			return
		case <-ticker.C:
			draw()
		}
	}
}

func readerTerminalFrame(status string, selected []string, projections []projection) string {
	byName := make(map[string]projection, len(projections))
	for _, p := range projections {
		byName[p.Name] = p
	}
	var b strings.Builder
	fmt.Fprintf(&b, "COUNTER READER / EVENT STREAM\nconnection: %s\n\n", status)
	fmt.Fprintln(&b, "color             value  actor state")
	for _, color := range selected {
		p, ok := byName[color]
		if !ok {
			fmt.Fprintf(&b, "%-10s %12s  waiting\n", color, "—")
			continue
		}
		residency := "paged out"
		if p.Resident {
			residency = "resident"
		}
		fmt.Fprintf(&b, "%-10s %12d  %s\n", color, p.Value, residency)
	}
	return b.String()
}
