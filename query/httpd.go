package query

import (
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/lodastack/log"
)

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

func (s *Service) initHandler() {
	s.router.GET("/ping", s.servePing)
	s.router.GET("/stats", s.statsHandler)
	s.router.GET("/series", s.seriesHandler)
	s.router.GET("/tags", s.tagsHandler)
	s.router.DELETE("/tags", s.tagsHandler)
	s.router.DELETE("/measurement", s.deleteMeasurementHandler)

	s.router.GET("/query", s.queryHandler)
	s.router.POST("/query", s.queryHandler)
	s.router.GET("/query2", s.query2Handler)
	s.router.POST("/query2", s.query2Handler)

	// custom API
	s.router.GET("/core", s.coreHandler)
}

// Service provides HTTP service.
type Service struct {
	addr string
	ln   net.Listener

	c      *Cache
	router *httprouter.Router

	logger *log.Logger
}

func New(listen string) (*Service, error) {
	return &Service{
		addr:   listen,
		c:      NewCache(),
		router: httprouter.New(),
	}, nil
}

func (s *Service) Start() {
	go s.c.purgeTimer()
	s.initHandler()
	server := http.Server{}
	server.Handler = accessLog(gzipFilter(cors(s.router)))
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic("Listen failed:" + err.Error())
	}
	s.ln = ln
	log.Infof("http start on %s!\n", s.addr)
	err = server.Serve(s.ln)
	if err != nil {
		fmt.Fprintf(os.Stderr, "http start failed:\n%s\n", err.Error())
	}
}
