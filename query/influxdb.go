package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lodastack/log"
	"github.com/lodastack/router/influx"
	"github.com/lodastack/router/loda"
)

func getTagsFromInfDb(ns, mt string) (map[string][]interface{}, error) {
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

func deleteTagsFromInfDb(ns string, mt string, tag string, tagvalue string) error {
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

	values, err := getMetricsFromInfDb(ns)
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

func getMetricsFromInfDb(ns string) ([]interface{}, error) {
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

func getSeriesFromInfDb(ns string) (map[string]map[string]detail, error) {
	values, err := getMetricsFromInfDb(ns)
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

	mNames := make(map[string]map[string]detail)
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
		d := Detail(mName)
		if _, ok := mNames[key]; !ok {
			mNames[key] = make(map[string]detail)
		}
		mNames[key][mName] = d
	}

	// only exist in registry, need display
	for _, m := range ms {
		if strings.HasPrefix(m.Name, "PLUGIN.") {
			mNameParts := strings.Split(m.Name, ".")
			key := transKey(mNameParts[0])
			d := Detail(m.Name)
			if _, ok := mNames[key]; !ok {
				mNames[key] = make(map[string]detail)
			}
			mNames[key][m.Name] = d
		}
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

func queryInfluxDb(influxdbs []string, params map[string][]string, ip string) (int, []byte, error) {
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

func quoteTags(tags string) (string, error) {
	rs := []string{}

	_tags := strings.Split(tags, ":")
	for _, tag := range _tags {
		r := []string{}

		kv := strings.Split(tag, "=")
		if len(kv) != 2 {
			return "", errors.New("invalid tags")
		}

		vs := strings.Split(kv[1], ",")
		for _, v := range vs {
			r = append(r, fmt.Sprintf("\"%s\"='%s'", kv[0], v))
		}

		rs = append(rs, "("+strings.Join(r, " or ")+")")
	}

	return strings.Join(rs, " and "), nil
}

func getSeriesViaTagsFromInfDb(ns, mt, tags string) ([]influx.SeriesObj, error) {
	influxdbs, err := loda.InfluxDBs(ns)
	if err != nil {
		return nil, err
	}

	if len(influxdbs) == 0 {
		return nil, fmt.Errorf("%s has no route config", ns)
	}

	var tagsClause string
	if len(tags) == 0 {
		tagsClause = ""
	} else {
		_tags, err := quoteTags(tags)
		if err != nil {
			return nil, err
		}
		tagsClause = " where " + _tags
	}

	rs, err := influx.Query(influxdbs, map[string]string{
		"db": ns,
		"q":  fmt.Sprintf("show series from \"%s\"%s", mt, tagsClause),
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

	return rs.Results[0].Series, nil
}
