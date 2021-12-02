package test

import (
	"errors"
	"io"
	"net/http"
)

type MockHttpClient struct {
	SentReqs     map[string]bool
	returnErrors bool
}

func NewMockHttpClient() *MockHttpClient {
	return &MockHttpClient{
		SentReqs: map[string]bool{},
	}
}

func (m *MockHttpClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	if m.returnErrors {
		return nil, errors.New("oops")
	}

	m.SentReqs[url] = true

	return &http.Response{}, nil
}

func (m *MockHttpClient) ReturnErrors() {
	m.returnErrors = true
}
