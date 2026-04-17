package main

import "net/http"

type ctxKey int

const requestIDKey ctxKey = 0

type loggingRW struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *loggingRW) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingRW) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	cnt, err := w.ResponseWriter.Write(b)
	w.bytes += cnt
	return cnt, err
}
