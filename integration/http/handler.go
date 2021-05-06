// +build integration

package http

import "net/http"

type Received map[string]bool

var Recvd Received

func init() {
	Recvd = map[string]bool{}
}

func GetHttpTestHandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/quitquitquit":
			handleQuit(w, r)
			return
		}
	}
}

func Reset() {
	Recvd = Received{}
}

func handleQuit(w http.ResponseWriter, r *http.Request) {
	Recvd["/quitquitquit"] = true

	w.WriteHeader(200)
}
