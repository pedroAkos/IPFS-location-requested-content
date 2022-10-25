package service

import (
	"io"
	"net/http"
)

func SendRequest(method string, url string, contentType string, body io.Reader) *http.Response {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(err)
	}
	req.Close = true
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}
