package query

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/lodastack/router/influx"
	"github.com/lodastack/router/loda"

	"github.com/lodastack/log"
)

// servePing returns a simple response to let the client know the server is running.
func servePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// @desc get measurement tags from influxdb deps on ns name
// @router /tags [get]
func tagsHandler(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" && req.Method != "DELETE" {
		errResp(resp, http.StatusMethodNotAllowed, "Get or delete please!")
		return
	}

	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}
	ns := params.Get("ns")
	mt := params.Get("measurement")
	tag := params.Get("tag")
	value := params.Get("value")
	if req.Method == "GET" {
		if ns == "" || mt == "" {
			errResp(resp, http.StatusBadRequest, "You need params 'ns=nsname&measurement=test'")
			return
		}

		tags, err := getTagsFromInfDb(ns, mt)
		if err != nil {
			errResp(resp, http.StatusInternalServerError, err.Error())
			return
		}
		succResp(resp, "OK", tags)
	}

	if req.Method == "DELETE" {
		if ns == "" || mt == "" || tag == "" || value == "" {
			errResp(resp, http.StatusBadRequest, "You need params")
			return
		}

		err := deleteTagsFromInfDb(ns, mt, tag, value)
		if err != nil {
			errResp(resp, http.StatusInternalServerError, err.Error())
			return
		}
		succResp(resp, "OK", nil)
	}

}

// @desc get series from influxdb deps on ns name
// @router /series [get]
func seriesHandler(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		errResp(resp, http.StatusMethodNotAllowed, "Get please!")
		return
	}

	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}
	ns := params.Get("ns")
	if ns == "" {
		errResp(resp, http.StatusBadRequest, "where is ns name?")
		return
	}
	series, err := getSeriesFromInfDb(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	succResp(resp, "OK", series)
}

// @desc drop measurement
// @router /measurement [delete]
func deleteMeasurementHandler(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "DELETE" {
		errResp(resp, http.StatusMethodNotAllowed, "Delete please!")
		return
	}

	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}

	ns := params.Get("ns")
	name := params.Get("name")
	regexp := params.Get("regexp")

	if ns == "" || name == "" {
		errResp(resp, http.StatusBadRequest, "ns or name please")
		return
	}

	var q string
	q = fmt.Sprintf("drop measurement \"%s\"", name)
	if regexp == "true" {
		q = fmt.Sprintf("DELETE FROM /^%s/", name)
	}

	ps := map[string]string{"q": q, "db": ns}
	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	if len(influxdbs) == 0 {
		errResp(resp, 400, ns+" has no influxdb route config")
		return
	}

	rs, err := influx.QueryRaw(influxdbs, ps, req.Header.Get("X-Real-IP"))
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	// just return the origin influxdb rs
	resp.Header().Add("Content-Type", "application/json")
	resp.WriteHeader(rs.Status)
	resp.Write(rs.Body)
}

// @desc origin query for influxdb
// @router /query [get]
func queryHandler(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" && req.Method != "POST" {
		errResp(resp, http.StatusMethodNotAllowed, "Get or Post please!")
		return
	}

	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}

	cluster := params.Get("cluster")
	_ns := params.Get("db")

	if len(_ns) == 0 {
		_ns, err = parseDB(params.Get("q"))
		if err != nil {
			errResp(resp, http.StatusBadRequest, err.Error())
			return
		}
	}

	var ns string
	if len(cluster) > 0 {
		ns = "_cluster_" + cluster
	} else {
		ns = _ns
	}

	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	if len(influxdbs) == 0 {
		errResp(resp, 400, ns+" has no influxdb route config")
		return
	}

	// remote cluster param
	delete(params, "cluster")

	status, rs, err := queryInfluxDb(influxdbs, params, req.Header.Get("X-Real-IP"))
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	// just return the origin influxdb rs
	resp.Header().Add("Content-Type", "application/json")
	resp.WriteHeader(status)
	resp.Write(rs)
}

func parseDB(q string) (string, error) {
	list := strings.Split(q, " ")
	for _, str := range list {
		if strings.HasPrefix(str, "\"collect.") {
			return str, nil
		}
	}
	return "", fmt.Errorf("can not found db from params q")
}

// @desc origin query for influxdb
// @router /query2 [get]
func query2Handler(resp http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" && req.Method != "POST" {
		errResp(resp, http.StatusMethodNotAllowed, "Get or POST please!")
		return
	}

	ns := req.FormValue("ns")
	where := req.FormValue("where")
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	measurement := req.FormValue("measurement")
	fn := req.FormValue("fn")
	fill := req.FormValue("fill")

	if len(ns) == 0 || len(starttime) == 0 || len(endtime) == 0 || len(measurement) == 0 {
		errResp(resp, http.StatusBadRequest, "need params")
		return
	}

	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	if len(influxdbs) == 0 {
		errResp(resp, 400, ns+" has no influxdb route config")
		return
	}

	tags, err := getTagsFromInfDb(ns, measurement)
	if err != nil {
		errResp(resp, 500, ns+" get tags failed: "+err.Error())
		return
	}

	if len(tags) > 4 {
		//test
		errResp(resp, 500, ns+" tag > 4")
		return
	}

	var tagkeys []string
	for tagkey := range tags {
		tagkeys = append(tagkeys, tagkey)
	}

	query, err := NewQuery(measurement, starttime, endtime, tagkeys, where, fn, fill)
	if err != nil {
		errResp(resp, 500, ns+" new query failed: "+err.Error())
		return
	}
	fmt.Println(query)

	p := url.Values{}
	p.Set("q", query)
	p.Set("db", ns)
	p.Set("epoch", "s")
	p.Set("pretty", "true")
	// p.Set("chunked", "true")
	// p.Set("chunk_size", "200000000000000000")
	req.URL.RawQuery = p.Encode()
	status, rs, err := queryInfluxDB(influxdbs, p, req.Header.Get("X-Real-IP"))
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	// just return the origin influxdb rs
	resp.Header().Add("Content-Type", "application/json")
	succResp(resp, "OK", rs)
	resp.WriteHeader(status)
}

// @desc health check
// @router /stats [get]
func statsHandler(resp http.ResponseWriter, req *http.Request) {
	succResp(resp, "OK", nil)
}

func coreHandler(resp http.ResponseWriter, req *http.Request) {
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	s, err := strconv.ParseInt(starttime, 10, 64)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}
	e, err := strconv.ParseInt(endtime, 10, 64)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	ns := "collect.api.loda"

	// DB route
	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	if len(influxdbs) == 0 {
		errResp(resp, 400, ns+" has no influxdb route config")
		return
	}

	series, err := getSeriesFromInfDb(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}
	m := make(map[string]float64)
	for name := range series["监控上报"] {
		if strings.HasSuffix(name, ".alive") {
			query := fmt.Sprintf("SELECT mean(\"value\") FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY time(%s)",
				name, starttime, endtime, "1m")
			p := url.Values{}
			p.Set("q", query)
			p.Set("db", ns)
			p.Set("epoch", "s")
			p.Set("pretty", "true")

			req.URL.RawQuery = p.Encode()
			_, rs, err := queryInfluxDB(influxdbs, p, req.Header.Get("X-Real-IP"))
			if err != nil {
				log.Errorf(err.Error())
				continue
			}
			var failedCount float64
			for _, result := range rs.Results {
				for _, serie := range result.Series {
					for _, pair := range serie.Values {
						if len(pair) == 2 {
							if v, ok := pair[1].(float64); ok {
								if v == 0 {
									failedCount++
								}
							}
						}
					}
				}
			}

			totalCount := float64((e - s) / 1000 / 60)
			m[name] = SetPrecision((totalCount-failedCount)/totalCount*100, 8)
			log.Debugf("failed conut: %v", failedCount)
		}
	}
	succResp(resp, "OK", m)
}
