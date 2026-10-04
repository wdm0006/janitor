package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func status(h http.Handler, path string) int {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestMetricsMuxServesOnlyHealthz(t *testing.T) {
	mux := newMetricsMux()
	if got := status(mux, "/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", got)
	}
	for _, p := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/heap", "/debug/pprof/profile", "/debug/vars"} {
		if got := status(mux, p); got != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", p, got)
		}
	}
}

func TestPprofMuxServesPprofOnly(t *testing.T) {
	mux := newPprofMux()
	for _, p := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/heap", "/debug/pprof/goroutine?debug=1"} {
		if got := status(mux, p); got != http.StatusOK {
			t.Errorf("%s = %d, want 200", p, got)
		}
	}
	if got := status(mux, "/healthz"); got != http.StatusNotFound {
		t.Errorf("/healthz = %d, want 404", got)
	}
}
