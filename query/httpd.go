package query

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/lodastack/router/config"

	"github.com/lodastack/log"
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

func accessLog(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stime := time.Now().UnixNano() / 1e3
		inner.ServeHTTP(w, r)
		dur := time.Now().UnixNano()/1e3 - stime
		if dur <= 1e3 {
			log.Infof("access %s path %s in %d us\n", r.Method, r.URL.Path, dur)
		} else {
			log.Infof("access %s path %s in %d ms\n", r.Method, r.URL.Path, dur/1e3)
		}
	})
}

func cors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
				`AuthToken`,
				`NS`,
				`Resource`,
				`X-Requested-With`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func addHandlers() {
	http.Handle("/ping", accessLog(cors(http.HandlerFunc(servePing))))
	http.Handle("/stats", accessLog(cors(http.HandlerFunc(statsHandler))))
	http.Handle("/series", accessLog(cors(http.HandlerFunc(seriesHandler))))
	http.Handle("/tags", accessLog(cors(http.HandlerFunc(tagsHandler))))
	http.Handle("/query", accessLog(cors(http.HandlerFunc(queryHandler))))
	http.Handle("/query2", accessLog(cors(http.HandlerFunc(query2Handler))))
	http.Handle("/measurement", accessLog(cors(http.HandlerFunc(deleteMeasurementHandler))))
}

func Start() {
	bind := fmt.Sprintf("%s", config.GetConfig().Com.Listen)
	log.Infof("http start on %s!\n", bind)

	addHandlers()

	err := http.ListenAndServe(bind, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http start failed:\n%s\n", err.Error())
	}
}
