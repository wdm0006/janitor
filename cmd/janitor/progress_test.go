package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	j "github.com/wdm0006/janitor/pkg/janitor"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func progressSchema() j.Schema {
	return j.Schema{Columns: []j.ColumnSchema{{Name: "k", Type: j.KindInt, Nullable: true}}}
}

func progressFrame(vals ...int64) *j.Frame {
	f := j.NewFrame(progressSchema())
	for i, v := range vals {
		f.AppendNullRow()
		c, _ := f.ColumnByName("k")
		c.(*j.IntColumn).Set(i, v)
	}
	return f
}

// gatedSource yields the first frame, then closes parked and blocks until release is closed.
type gatedSource struct {
	first   *j.Frame
	given   bool
	parked  chan struct{}
	release chan struct{}
}

func (g *gatedSource) Next() (*j.Frame, error) {
	if !g.given {
		g.given = true
		return g.first, nil
	}
	close(g.parked)
	<-g.release
	return nil, io.EOF
}

type signalSink struct{}

func (s *signalSink) Write(*j.Frame) error { return nil }
func (s *signalSink) Close() error         { return nil }

func newGated(first *j.Frame) *gatedSource {
	return &gatedSource{first: first, parked: make(chan struct{}), release: make(chan struct{})}
}

// observeTicks lets the first chunk finish, delivers two ticks while the worker is parked
// on the next chunk (the second send returns only after the first report completed), then
// releases the source and returns the stream result plus the progress output.
func observeTicks(t *testing.T, run func(src j.ChunkSource, tick <-chan time.Time, w io.Writer) error, src *gatedSource) string {
	t.Helper()
	tick := make(chan time.Time)
	out := &lockedBuf{}
	errc := make(chan error, 1)
	go func() { errc <- run(src, tick, out) }()
	<-src.parked
	tick <- time.Now()
	tick <- time.Now()
	close(src.release)
	if err := <-errc; err != nil {
		t.Fatalf("stream error: %v", err)
	}
	return out.String()
}

func TestStreamWithProgressReportsActiveWorker(t *testing.T) {
	sink := &signalSink{}
	src := newGated(progressFrame(1, 2, 3))
	out := observeTicks(t, func(s j.ChunkSource, tick <-chan time.Time, w io.Writer) error {
		return streamWithProgress(context.Background(), j.NewPipeline(), s, sink, 6, false, tick, w)
	}, src)

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 progress lines, got %q", out)
	}
	for _, l := range lines {
		if !strings.Contains(l, " 50.0% rows=3/6 ") {
			t.Errorf("line %q lacks 50.0%% rows=3/6", l)
		}
	}
}

func TestStreamWithProgressJSONSnapshot(t *testing.T) {
	sink := &signalSink{}
	src := newGated(progressFrame(1, 2, 3, 4))
	out := observeTicks(t, func(s j.ChunkSource, tick <-chan time.Time, w io.Writer) error {
		return streamWithProgress(context.Background(), j.NewPipeline(), s, sink, 0, true, tick, w)
	}, src)

	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		var evt map[string]any
		if err := json.Unmarshal([]byte(l), &evt); err != nil {
			t.Fatalf("bad json %q: %v", l, err)
		}
		if evt["type"] != "progress" || evt["rows"] != float64(4) {
			t.Errorf("unexpected event %v", evt)
		}
		if r, _ := evt["rate_rps"].(float64); r <= 0 {
			t.Errorf("rate_rps = %v, want > 0", evt["rate_rps"])
		}
	}
}

func TestStreamPartitionedReportsActiveWorker(t *testing.T) {
	sink := &signalSink{}
	src := newGated(progressFrame(1, 1, 2))
	makeSink := func(string, j.Schema) (j.ChunkSink, error) { return sink, nil }
	out := observeTicks(t, func(s j.ChunkSource, tick <-chan time.Time, w io.Writer) error {
		return streamPartitioned(context.Background(), j.NewPipeline(), s, "out-{col:k}.csv", makeSink, progressSchema(), []string{"k"}, true, 3, false, tick, w)
	}, src)

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 progress lines, got %q", out)
	}
	for _, l := range lines {
		if !strings.Contains(l, "100.0% rows=3/3 ") {
			t.Errorf("line %q lacks 100.0%% rows=3/3", l)
		}
	}
}
