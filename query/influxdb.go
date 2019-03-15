package query

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/grafana/grafana/pkg/tsdb"
	"github.com/lodastack/log"
	"github.com/lodastack/router/influx"
	"github.com/lodastack/router/loda"
)

// NewQuery only return about 1500 points
func NewQuery(measurement string, start string, end string, tags []string, where string, fn string, fill string) (string, error) {

	tr := tsdb.NewTimeRange(start, end)
	interval := tsdb.CalculateInterval(tr)

	if fn == "" {
		fn = "mean"
	}
	if fill == "" {
		fill = "null"
	}

	var filterTags []string
	for _, tagkey := range tags {
		if strings.Contains(where, tagkey) {
			filterTags = append(filterTags, fmt.Sprintf("\"%s\"", tagkey))
		}
	}

	rawQuery := fmt.Sprintf("SELECT %s(\"value\") FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY time(%s) fill(%s)",
		fn, measurement, start, end, interval, fill)
	// display hostname if fn in (max, min, medium)
	if fn == "max" || fn == "min" || fn == "medium" {
		rawQuery = fmt.Sprintf("SELECT %s(\"value\"),\"host\" FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY time(%s) fill(%s)",
			fn, measurement, start, end, interval, fill)
	}

	if where != "" {
		rawQuery = fmt.Sprintf("SELECT %s(\"value\") FROM \"%s\" WHERE %s AND time > %sms and time < %sms GROUP BY time(%s), %s fill(%s)",
			fn, measurement, where, start, end, interval, strings.Join(filterTags, ","), fill)
	}
	return rawQuery, nil
}

// NewUsageQuery is customer API for customer system
func NewUsageQuery(measurement, fn, period, duration, stime, etime string, groupby []string) (string, error) {
	//select max("value") from "cpu.idle" where time> now() - 1d group by "host","tag",time(1h);
	groupBy := "\"host\""
	for _, tag := range groupby {
		groupBy = groupBy + ",\"" + tag + "\""
	}

	rawQuery := fmt.Sprintf("SELECT %s(\"value\") FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY %s,time(%s)",
		fn, measurement, stime, etime, groupBy, duration)
	return rawQuery, nil
}

func tags(ns, mt string) (map[string][]interface{}, error) {
	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		return nil, err
	}

	if len(influxdbs) == 0 {
		return nil, fmt.Errorf("%s has no route config", ns)
	}

	rs, err := influx.Query(influxdbs, map[string]string{
		"db": ns,
		"q":  fmt.Sprintf("show tag keys from \"%s\"", mt),
	}, "")
	if err != nil {
		return nil, err
	}
	if len(rs.Results) == 0 {
		return nil, nil
	}
	if len(rs.Results[0].Series) == 0 {
		return nil, nil
	}

	var tagKeys []string
	for _, values := range rs.Results[0].Series[0].Values {
		_values, ok := values.([]interface{})
		if !ok {
			continue
		}
		if len(_values) == 0 {
			continue
		}
		value, ok := _values[0].(string)
		tagKeys = append(tagKeys, "\""+value+"\"")
	}

	keys := "(" + strings.Join(tagKeys, ",") + ")"

	rs, err = influx.Query(influxdbs, map[string]string{
		"db": ns,
		"q":  fmt.Sprintf("show tag values from \"%s\" with key in %s", mt, keys),
	}, "")
	if err != nil {
		return nil, err
	}
	if len(rs.Results) == 0 {
		return nil, nil
	}
	if len(rs.Results[0].Series) == 0 {
		return nil, nil
	}
	tagsMap := make(map[string][]interface{})

	for _, s := range rs.Results[0].Series {
		if len(s.Columns) == 0 {
			continue
		}

		for _, vs := range s.Values {
			if v, ok := vs.([]interface{}); !ok {
				continue
			} else {
				if len(v) == 0 || len(v) < 2 {
					continue
				}
				key, ok := v[0].(string)
				if !ok {
					continue
				}
				value, ok := v[1].(string)
				if !ok {
					continue
				}
				tagsMap[key] = append(tagsMap[key], value)
			}
		}
	}
	return tagsMap, nil
}

func removeTags(ns string, mt string, tag string, tagvalue string) error {
	if tag != "host" {
		return nil
	}

	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		return err
	}

	if len(influxdbs) == 0 {
		return fmt.Errorf("%s has no route config", ns)
	}

	values, err := getMeasurementsFromInfluxDB(ns)
	if err != nil {
		return err
	}

	if len(values) == 0 {
		return nil
	}

	for _, value := range values {
		v, ok := value.([]interface{})
		if !ok || len(v) == 0 {
			continue
		}
		mName, ok := v[0].(string)
		if !ok {
			continue
		}

		_, err = influx.Query(influxdbs, map[string]string{
			"db": ns,
			"q":  fmt.Sprintf("DELETE FROM \"%s\" WHERE \"%s\" = '%s'", mName, tag, tagvalue),
		}, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func getMeasurementsFromInfluxDB(ns string) ([]interface{}, error) {
	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		return nil, err
	}

	if len(influxdbs) == 0 {
		return nil, fmt.Errorf("%s has no route config", ns)
	}

	rs, err := influx.Query(influxdbs, map[string]string{
		"db": ns,
		"q":  "show measurements",
	}, "")

	if err != nil {
		return nil, err
	}

	if rs == nil || len(rs.Results) == 0 {
		return nil, nil
	}

	if len(rs.Results[0].Series) == 0 {
		return nil, nil
	}

	return rs.Results[0].Series[0].Values, nil
}

func measurements(ns string) (map[string]map[string]Detail, error) {
	values, err := getMeasurementsFromInfluxDB(ns)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	// get all collects from registry
	ms, err := loda.CollectMetrics(ns)
	if err != nil {
		log.Error(err)
	}

	mNames := make(map[string]map[string]Detail)
	for _, value := range values {
		v, ok := value.([]interface{})
		if !ok || len(v) == 0 {
			continue
		}
		mName, ok := v[0].(string)
		if !ok {
			continue
		}

		if strings.HasPrefix(mName, "_") {
			continue
		}

		if !existInRegistry(mName, ms) {
			continue
		}

		mNameParts := strings.Split(mName, ".")
		key := transKey(mNameParts[0])
		d := MeasurementDetail(mName)
		if _, ok := mNames[key]; !ok {
			mNames[key] = make(map[string]Detail)
		}
		mNames[key][mName] = d
	}
	return mNames, nil
}

func existInRegistry(metricName string, ms []loda.CollectMetric) bool {
	if len(ms) == 0 {
		return true
	}
	for _, m := range ms {
		if strings.HasPrefix(metricName, "RUN.") {
			return true
		}
		if strings.HasPrefix(metricName, m.Name) {
			return true
		}
	}
	return false
}

func queryInfluxRaw(influxdbs []string, params map[string][]string, ip string) (int, []byte, error) {
	queryParams := make(map[string]string)
	for k, v := range params {
		if len(v) == 0 {
			continue
		}
		queryParams[k] = v[0]
	}

	response, err := influx.QueryRaw(influxdbs, queryParams, ip)
	if err != nil {
		return 0, nil, err
	}
	return response.Status, response.Body, nil
}

// Results struct
type Results struct {
	Results []Result
	Err     error
}

// Result struct
type Result struct {
	Series   []Row
	Messages []*Message
	Err      error
}

// Message struct
type Message struct {
	Level string `json:"level,omitempty"`
	Text  string `json:"text,omitempty"`
}

// Row struct
type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Data    [][]interface{}   `json:"data,omitempty"`
}

func queryInfluxDB(influxdbs []string, params map[string][]string, ip string, needParse bool) (int, Results, error) {
	var response Results
	queryParams := make(map[string]string)
	for k, v := range params {
		if len(v) == 0 {
			continue
		}
		queryParams[k] = v[0]
	}
	resp, err := httpDo(influxdbs, queryParams, ip)
	if err != nil {
		return 500, response, err
	}

	dec := json.NewDecoder(resp.Body)
	defer resp.Body.Close()
	//dec.UseNumber()
	err = dec.Decode(&response)

	if err != nil {
		return 500, response, err
	}

	if response.Err != nil {
		return 500, response, response.Err
	}

	if needParse {
		res := parse(&response)
		return resp.StatusCode, *res, nil
	}

	return resp.StatusCode, response, nil
}

func httpDo(hosts []string, params map[string]string, ip string) (*http.Response, error) {
	var host string
	if len(hosts) > 0 {
		host = hosts[0]
	}
	fullURL := fmt.Sprintf("%s%s", influx.GetQueryUrl(host), influx.ParseParams(params))
	resp, err := http.Get(fullURL)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func parse(response *Results) *Results {
	for i, result := range response.Results {
		for j, serie := range result.Series {
			for _, pair := range serie.Values {
				if v, ok := pair[1].(float64); ok {
					var p []interface{}
					p = append(p, pair[0])
					p = append(p, SetPrecision(v, 4))
					if len(pair) > 2 {
						for _, item := range pair[2:] {
							p = append(p, item)
						}
					}
					response.Results[i].Series[j].Data = append(response.Results[i].Series[j].Data, p)
				}
			}
			response.Results[i].Series[j].Values = response.Results[i].Series[j].Data
			response.Results[i].Series[j].Data = nil
		}
	}
	return response
}
