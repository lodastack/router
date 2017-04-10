package query

import (
	"encoding/json"
	"net/http"
	"time"
)

type Response struct {
	StatusCode int         `json:"httpstatus"`
	Msg        string      `json:"msg"`
	Data       interface{} `json:"data"`
}

func errResp(resp http.ResponseWriter, status int, msg string) {
	response := Response{
		StatusCode: status,
		Msg:        msg,
		Data:       nil,
	}
	bytes, _ := json.Marshal(&response)
	resp.Header().Add("Content-Type", "application/json")
	resp.WriteHeader(status)
	resp.Write(bytes)
}

func succResp(resp http.ResponseWriter, msg string, data interface{}) {
	response := Response{
		StatusCode: http.StatusOK,
		Msg:        msg,
		Data:       data,
	}
	bytes, _ := json.Marshal(&response)
	resp.Header().Add("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	resp.Write(bytes)
}

func getTimeDurMs(start time.Time, end time.Time) float64 {
	return float64((end.UnixNano() - start.UnixNano()) / 1e6)
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

func (this *responseWriter) WriteHeader(code int) {
	this.statusCode = code
	this.ResponseWriter.WriteHeader(code)
}
