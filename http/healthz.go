package http

import (
	"net"
	"net/http"
	"time"

	"inviqa/kafka-outbox-relay/log"
)

type healthzHandler struct {
	checkAddr []string
	db        Pinger
}

type Pinger interface {
	Ping() error
}

func NewHealthzHandler(checkAddr []string, db Pinger) http.Handler {
	return &healthzHandler{
		checkAddr: checkAddr,
		db:        db,
	}
}

func (h healthzHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	healthy := true
	if req.URL.Query().Get("readiness") == "1" {
		healthy = h.checkServices() && h.checkDatabase()
	} else {
		healthy = h.checkDatabase()
	}

	if healthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (h healthzHandler) checkDatabase() bool {
	if err := h.db.Ping(); err != nil {
		log.Logger.Debug("database is not available or there is a problem with connectivity")
		return false
	}
	return true
}

func (h healthzHandler) checkServices() bool {
	healthy := true
	for _, host := range h.checkAddr {
		log.Logger.Debugf("checking connectivity to %s", host)
		conn, err := net.DialTimeout("tcp", host, 1*time.Second)
		if err != nil {
			healthy = false
			log.Logger.Debugf("unable to connect to %s", host)
		} else {
			_ = conn.Close()
		}
	}
	return healthy
}
