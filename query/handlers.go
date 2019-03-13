package query

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/lodastack/router/config"
	"github.com/lodastack/router/influx"
	"github.com/lodastack/router/loda"

	"github.com/julienschmidt/httprouter"
	"github.com/lodastack/log"
)

// servePing returns a simple response to let the client know the server is running.
func (s *Service) servePing(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusNoContent)
}

// listTagsHandler list tags
func (s *Service) listTagsHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}
	ns := params.Get("ns")
	mt := params.Get("measurement")

	if ns == "" || mt == "" {
		errResp(resp, http.StatusBadRequest, "You need params 'ns=nsname&measurement=test'")
		return
	}

	tags, err := tags(ns, mt)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}
	succResp(resp, "OK", tags)
}

func (s *Service) removeTagsHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}
	ns := params.Get("ns")
	mt := params.Get("measurement")
	tag := params.Get("tag")
	value := params.Get("value")
	if ns == "" || mt == "" || tag == "" || value == "" {
		errResp(resp, http.StatusBadRequest, "You need params")
		return
	}
	err = removeTags(ns, mt, tag, value)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}
	succResp(resp, "OK", nil)
}

func (s *Service) listMeasurementHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
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
	series, err := measurements(ns)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	succResp(resp, "OK", series)
}

// remove measurement
func (s *Service) removeMeasurementHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
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

func (s *Service) queryHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	if req.Method != "GET" && req.Method != "POST" {
		errResp(resp, http.StatusMethodNotAllowed, "Get or Post please!")
		return
	}

	params, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(resp, http.StatusBadRequest, err.Error())
		return
	}

	if strings.Contains(strings.ToLower(params.Get("q")), "drop ") || strings.Contains(strings.ToLower(params.Get("q")), "delete ") {
		errResp(resp, http.StatusBadRequest, "ah, Don't support drop")
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

	status, rs, err := queryInfluxRaw(influxdbs, params, req.Header.Get("X-Real-IP"))
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
		if !strings.HasPrefix(str, "\""+config.GetConfig().Nsq.TopicPrefix+".") {
			continue
		}
		if dbIndex := strings.Index(str, "\"."); dbIndex != -1 {
			return str[1:dbIndex], nil
		}
	}
	return "", fmt.Errorf("can not found db from params q")
}

func (s *Service) query2Handler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
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

	tags, err := tags(ns, measurement)
	if err != nil {
		errResp(resp, 500, ns+" get tags failed: "+err.Error())
		return
	}

	if len(tags) > 10 {
		//tags num can not grater than 10
		errResp(resp, 500, ns+" tag > 10")
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

	p := url.Values{}
	p.Set("q", query)
	p.Set("db", ns)
	p.Set("epoch", "s")
	p.Set("pretty", "true")

	req.URL.RawQuery = p.Encode()
	status, rs, err := queryInfluxDB(influxdbs, p, req.Header.Get("X-Real-IP"), false)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	// just return the origin influxdb rs
	resp.Header().Add("Content-Type", "application/json")
	succResp(resp, "OK", rs)
	resp.WriteHeader(status)
}

func (s *Service) statsHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	succResp(resp, "OK", nil)
}

func (s *Service) saHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	ns := req.FormValue("ns")

	if ns == "" {
		ns = config.GetConfig().Com.DefaultAPINameSpace
	}

	if ns == config.GetConfig().Com.DefaultAPINameSpace {
		if res := s.c.Get(config.GetConfig().Nsq.TopicPrefix + "." + ns + starttime + endtime); res != nil {
			succResp(resp, "OK", res)
			return
		}
	}

	ns = config.GetConfig().Nsq.TopicPrefix + "." + ns
	m, err := s.sa(ns, starttime, endtime)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}
	succResp(resp, "OK", m)
}

func (s *Service) sa(ns string, starttime string, endtime string) (map[string]float64, error) {
	m := make(map[string]float64)
	st, err := strconv.ParseInt(starttime, 10, 64)
	if err != nil {
		return m, err
	}
	et, err := strconv.ParseInt(endtime, 10, 64)
	if err != nil {
		return m, err
	}

	// DB route
	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		return m, err
	}

	if len(influxdbs) == 0 {
		return m, fmt.Errorf(ns + " has no influxdb route config")
	}

	series, err := measurements(ns)
	if err != nil {
		return m, err
	}

	for name := range series["SDK"] {
		if strings.HasSuffix(name, ".alive") {
			query := fmt.Sprintf("select * from (SELECT mean(\"value\") FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY time(%s)) where \"mean\"=0",
				name, starttime, endtime, "1m")
			p := url.Values{}
			p.Set("q", query)
			p.Set("db", ns)
			p.Set("epoch", "s")
			p.Set("pretty", "true")

			p.Encode()
			_, rs, err := queryInfluxDB(influxdbs, p, "", false)
			if err != nil {
				log.Errorf(err.Error())
				continue
			}
			var failedCount float64
			for _, result := range rs.Results {
				for _, serie := range result.Series {
					failedCount = failedCount + float64(len(serie.Values))
				}
			}

			totalCount := math.Ceil(float64(et-st) / 1000 / 60)
			m[name] = SetPrecision((totalCount-failedCount)/totalCount*100, 8)
			log.Debugf("failed conut: %v", failedCount)
		}
	}
	s.c.Set(ns+starttime+endtime, m)
	return m, nil
}

func (s *Service) usageHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	if !config.GetConfig().Usg.Enable {
		errResp(resp, 403, "no permission")
		return
	}

	ns := req.FormValue("ns")
	measurement := req.FormValue("measurement")
	fn := req.FormValue("fn")
	period := req.FormValue("period")
	duration := req.FormValue("duration")
	groupby := req.FormValue("groupby")

	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")

	st, err := strconv.ParseInt(starttime, 10, 64)
	if err != nil {
		log.Errorf("[usage] %s starttime format error", starttime)
		errResp(resp, http.StatusBadRequest, "params error")
		return
	}
	et, err := strconv.ParseInt(endtime, 10, 64)
	if err != nil {
		log.Errorf("[usage] %s endtime format error", endtime)
		errResp(resp, http.StatusBadRequest, "params error")
		return
	}

	// can not greater than 1day(unit:ms)
	if et-st > 24*60*60*1000 {
		log.Errorf("[usage] %s - %s > 1d", endtime, starttime)
		errResp(resp, http.StatusBadRequest, "params error")
		return
	}

	//period = "1d"
	duration = "1h"

	if len(ns) == 0 || len(measurement) == 0 || len(fn) == 0 {
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

	tags, err := tags(ns, measurement)
	if err != nil {
		errResp(resp, 500, ns+" get tags failed: "+err.Error())
		return
	}

	if len(tags) > 10 {
		errResp(resp, 500, ns+" tag > 10")
		return
	}

	var tagkeys []string
	for tagkey := range tags {
		tagkeys = append(tagkeys, tagkey)
	}

	var groupByList []string
	if groupby != "" {
		groupByList = strings.Split(groupby, ",")
	}
	query, err := NewUsageQuery(measurement, fn, period, duration, starttime, endtime, groupByList)
	if err != nil {
		errResp(resp, 500, ns+" new query failed: "+err.Error())
		return
	}
	log.Errorf("[usage] query: %s", query)
	p := url.Values{}
	p.Set("q", query)
	p.Set("db", ns)
	p.Set("epoch", "s")
	p.Set("pretty", "true")

	req.URL.RawQuery = p.Encode()
	status, rs, err := queryInfluxDB(influxdbs, p, req.Header.Get("X-Real-IP"), true)
	if err != nil {
		errResp(resp, http.StatusInternalServerError, err.Error())
		return
	}

	// just return the origin influxdb rs
	resp.Header().Add("Content-Type", "application/json")
	succResp(resp, "OK", rs)
	resp.WriteHeader(status)
}

func (s *Service) linkstatsHandler(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {

	/*
	   {
	   	"nodes": [
	   		{"id": "OldMan1"},
	   		{"id": "OldMan2"},
	   		{"id": "OldMan3"},
	   		{"id": "OldMan4"},
	   		{"id": "OldMan5"},
	   	],
	   	"links": [
	   		{"source": "OldMan1", "target": "OldMan1"},
	   		{"source": "OldMan1", "target": "OldMan2"},
	   		{"source": "OldMan1", "target": "OldMan3"},
	   		{"source": "OldMan1", "target": "OldMan4"},
	   		{"source": "OldMan1", "target": "OldMan5"},
	   	]
	   }
	*/

	type node struct {
		ID   string `json:"id"`
		Host string `json:"host"`
		Type int    `json:"type"`
	}
	type link struct {
		Source string  `json:"source"`
		Target string  `json:"target"`
		Value  float64 `json:"value"`
	}
	type Res struct {
		Nodes []node `json:"nodes"`
		Links []link `json:"links"`
	}

	var reses []Res
	measurement := "RUN.ping.loss"
	for _, ns := range config.GetConfig().LinkStats.NS {
		if ns == "" {
			continue
		}
		// for remove rep
		idcmap := make(map[string]node)
		linkmap := make(map[string]link)
		var res Res
		var nodes []node
		var links []link
		ens := config.GetConfig().Nsq.TopicPrefix + "." + ns
		influxdbs, err := loda.InfluxDBs(ens)
		if err != nil {
			errResp(resp, http.StatusInternalServerError, err.Error())
			return
		}

		if len(influxdbs) == 0 {
			errResp(resp, 400, ens+" has no influxdb route config")
			return
		}

		tags, err := tags(ens, measurement)
		if err != nil {
			errResp(resp, 500, ens+" get tags failed: "+err.Error())
			return
		}

		if len(tags) > 10 {
			errResp(resp, 500, ens+" tag > 10")
			return
		}

		var sources, targets []string
		for k, v := range tags {
			if k == "from" {
				for _, source := range v {
					if sourceString, ok := source.(string); ok {
						//nodes = append(nodes, node{ID: sourceString, Type: 0})
						if idcname := idc(sourceString); idcname != "" {
							idcmap[idcname] = node{ID: idcname, Host: sourceString, Type: 0}
							sources = append(sources, sourceString)
						}
					}
				}
			}
			if k == "host" {
				for _, target := range v {
					if targetString, ok := target.(string); ok {
						//nodes = append(nodes, node{ID: targetString, Type: 1})
						if idcname := idc(targetString); idcname != "" {
							idcmap[idcname] = node{ID: idcname, Host: targetString, Type: 1}
							targets = append(targets, targetString)
						}
					}
				}
			}
		}

		for _, s := range sources {
			for _, t := range targets {
				value, err := latest(influxdbs, ens, measurement, s, t)
				if err != nil {
					continue
				}
				//links = append(links, link{Source: s, Target: t, Value: value})
				sidc := idc(s)
				tidc := idc(t)
				if sidc == tidc {
					continue
				}
				linkmap[sidc+tidc] = link{Source: sidc, Target: tidc, Value: value}
			}
		}

		for _, v := range idcmap {
			nodes = append(nodes, v)
		}
		for _, v := range linkmap {
			links = append(links, v)
		}
		res.Links, res.Nodes = links, nodes
		reses = append(reses, res)
	}
	resp.Header().Add("Content-Type", "application/json")
	succResp(resp, "OK", reses)
}

func idc(h string) string {
	for _, idc := range config.GetConfig().IDC {
		for _, host := range idc.Hosts {
			if host == h {
				return idc.Name
			}
		}
	}
	return ""
}

func latest(influxdbs []string, ns, measurement, source, target string) (float64, error) {
	query := fmt.Sprintf("select LAST(\"value\") from \"%s\" where \"host\" = '%s' and \"from\" = '%s'", measurement, target, source)
	p := url.Values{}
	p.Set("q", query)
	p.Set("db", ns)
	p.Set("epoch", "s")
	p.Set("pretty", "true")

	p.Encode()
	_, rs, err := queryInfluxDB(influxdbs, p, "", false)
	if err != nil {
		log.Errorf(err.Error())
		return 0, err
	}

	for _, result := range rs.Results {
		for _, serie := range result.Series {
			for _, pair := range serie.Values {
				if len(pair) == 2 {
					if v, ok := pair[1].(float64); ok {
						return SetPrecision(v, 4), nil
					}
				}
			}
		}
	}

	return 0, fmt.Errorf("not found value")
}
