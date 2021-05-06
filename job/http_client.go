package job

import (
	"io"
	"net/http"
)

type httpPoster interface {
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
}
