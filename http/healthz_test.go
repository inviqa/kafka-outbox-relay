package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewHealthzHandler(t *testing.T) {
	if nil == NewHealthzHandler([]string{""}, &mockPinger{}) {
		t.Errorf("got nil, expected a http.Handler instance")
	}
}

func TestHealthzHandler_ServeHTTP_ReadinessWhenHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz?readiness=1", nil)
	handler := NewHealthzHandler([]string{strings.Replace(srv.URL, "http://", "", 1)}, &mockPinger{})
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected 200 response code, but got %d", recorder.Code)
	}
}

func TestHealthzHandler_ServeHTTP_ReadinessWhenServiceUnhealthy(t *testing.T) {
	recorder := httptest.NewRecorder()
	handler := NewHealthzHandler([]string{"foo:9090"}, &mockPinger{})
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz?readiness=1", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 response code, but got %d", recorder.Code)
	}
}

func TestHealthzHandler_ServeHTTP_ReadinessWhenDbUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	recorder := httptest.NewRecorder()
	mp := &mockPinger{}
	mp.enableErrors()

	handler := NewHealthzHandler([]string{strings.Replace(srv.URL, "http://", "", 1)}, mp)
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz?readiness=1", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 response code, but got %d", recorder.Code)
	}
}

func TestHealthzHandler_ServeHTTP_LivenessWhenHealthy(t *testing.T) {
	recorder := httptest.NewRecorder()
	mp := &mockPinger{}
	handler := NewHealthzHandler([]string{"http://foo:9090"}, mp)
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Errorf("expected 200 response code, but got %d", recorder.Code)
	}
}

func TestHealthzHandler_ServeHTTP_LivenessWhenDbUnavailable(t *testing.T) {
	recorder := httptest.NewRecorder()
	mp := &mockPinger{}
	mp.enableErrors()
	handler := NewHealthzHandler([]string{"http://foo:9090"}, mp)
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 response code, but got %d", recorder.Code)
	}
}

type mockPinger struct {
	error bool
}

func (m *mockPinger) enableErrors() {
	m.error = true
}

func (m *mockPinger) Ping() error {
	if m.error {
		return errors.New("oops")
	}
	return nil
}
