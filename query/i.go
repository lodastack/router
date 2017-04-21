package query

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/grafana/grafana/pkg/tsdb"
	"github.com/lodastack/router/influx"
)

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
		filterTags = append(filterTags, fmt.Sprintf("\"%s\"", tagkey))
	}

	rawQuery := fmt.Sprintf("SELECT %s(\"value\") FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY time(%s) fill(%s)",
		fn, measurement, start, end, interval, fill)
	if where != "" {
		rawQuery = fmt.Sprintf("SELECT %s(\"value\") FROM \"%s\" WHERE %s AND time > %sms and time < %sms GROUP BY time(%s), %s fill(%s)",
			fn, measurement, where, start, end, interval, strings.Join(filterTags, ","), fill)
	}
	return rawQuery, nil
}

func NewUsageQuery(measurement string, fn string, period string, duration string, stime string, etime string) (string, error) {
	//select max("value") from "cpu.idle" where time> now() - 1d group by "host",time(1h);
	rawQuery := fmt.Sprintf("SELECT %s(\"value\") FROM \"%s\" WHERE time > %sms and time < %sms GROUP BY \"host\",time(%s)",
		fn, measurement, stime, etime, duration)
	return rawQuery, nil
}

type QueryResult struct {
	Results []Result
	Err     error
}

type Result struct {
	Series   []Row
	Messages []*Message
	Err      error
}

type Message struct {
	Level string `json:"level,omitempty"`
	Text  string `json:"text,omitempty"`
}

type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Data    []Point           `json:"data,omitempty"`
}

type Point struct {
	Time  interface{} `json:"time"`
	Value interface{} `json:"value"`
}

func queryInfluxDB(influxdbs []string, params map[string][]string, ip string, parse bool) (int, QueryResult, error) {
	var response QueryResult
	queryParams := make(map[string]string)
	for k, v := range params {
		if len(v) == 0 {
			continue
		}
		queryParams[k] = v[0]
	}
	resp, err := HttpDo(influxdbs, queryParams, ip)
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

	if parse {
		res := Parse(&response)
		return resp.StatusCode, *res, nil
	}

	return resp.StatusCode, response, nil
}

func HttpDo(hosts []string, params map[string]string, ip string) (*http.Response, error) {
	var host string
	if len(hosts) > 0 {
		host = hosts[0]
	}
	fullUrl := fmt.Sprintf("%s%s", influx.GetQueryUrl(host), influx.ParseParams(params))
	resp, err := http.Get(fullUrl)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func Parse(response *QueryResult) *QueryResult {
	for i, result := range response.Results {
		for j, serie := range result.Series {
			for _, pair := range serie.Values {
				if len(pair) == 2 {
					if v, ok := pair[1].(float64); ok {
						var p Point
						p.Time = pair[0]
						p.Value = SetPrecision(v, 4)
						response.Results[i].Series[j].Data = append(response.Results[i].Series[j].Data, p)
						response.Results[i].Series[j].Values = nil
					}
				}
			}
		}
	}
	return response
}
