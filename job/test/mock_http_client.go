package test

import (
	"errors"
	"io"
	"net/http"
)

type mockHttpClient struct {
	SentReqs     map[string]bool
	returnErrors bool
}

func NewMockHttpClient() *mockHttpClient {
	return &mockHttpClient{
		SentReqs: map[string]bool{},
	}
}

func (m *mockHttpClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	if m.returnErrors {
		return nil, errors.New("oops")
	}

	m.SentReqs[url] = true

	return &http.Response{}, nil
}

func (m *mockHttpClient) ReturnErrors() {
	m.returnErrors = true
}
