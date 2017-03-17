package query

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
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

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

// WriteHeader sets the provided code as the response status. If the
// specified status is 204 No Content, then the Content-Encoding header
// is removed from the response, to prevent clients expecting gzipped
// encoded bodies from trying to deflate an empty response.
func (w gzipResponseWriter) WriteHeader(code int) {
	if code != http.StatusNoContent {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

// gzipFilter determines if the client can accept compressed responses, and encodes accordingly.
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}
		gz := getGzipWriter(w)
		defer putGzipWriter(gz)
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
	})
}

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

func getGzipWriter(w io.Writer) *gzip.Writer {
	gz := gzipWriterPool.Get().(*gzip.Writer)
	gz.Reset(w)
	return gz
}

func putGzipWriter(gz *gzip.Writer) {
	gz.Close()
	gzipWriterPool.Put(gz)
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
	http.Handle("/ping", accessLog(gzipFilter(cors(http.HandlerFunc(servePing)))))
	http.Handle("/stats", accessLog(gzipFilter(cors(http.HandlerFunc(statsHandler)))))
	http.Handle("/series", accessLog(gzipFilter(cors(http.HandlerFunc(seriesHandler)))))
	http.Handle("/tags", accessLog(gzipFilter(cors(http.HandlerFunc(tagsHandler)))))
	http.Handle("/query", accessLog(gzipFilter(cors(http.HandlerFunc(queryHandler)))))
	http.Handle("/query2", accessLog(gzipFilter(cors(http.HandlerFunc(query2Handler)))))
	http.Handle("/measurement", accessLog(gzipFilter(cors(http.HandlerFunc(deleteMeasurementHandler)))))
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
