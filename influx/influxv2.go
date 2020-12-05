package influx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/lodastack/router/config"
	"github.com/lodastack/router/loda"
	"github.com/lodastack/router/models"

	"github.com/lodastack/log"
)

func getURLv2(host string, module string) string {
	return fmt.Sprintf("http://%s:%d/api/v2/%s", IntranetIP(host), config.GetConfig().TSDB.Port, module)
}

// WritePointsv2 writes points into influxdb v2.
func WritePointsv2(influxDbs []string, pointsObj models.Points, try bool) error {
	db := pointsObj.Database
	precision := "ns"

	pointsCnt := len(pointsObj.Points)
	points := convLinePointv2(pointsObj.Points)
	data := []byte(strings.Join(points, "\n"))

	var influxDb string
	if len(influxDbs) > 0 {
		influxDb = influxDbs[0]
	} else {
		return fmt.Errorf("no db config")
	}
	// write data to mutile DBs
	if len(influxDbs) > 1 {
		for _, indexDB := range influxDbs[1:] {
			//limit.Take()
			go writePointsv2(indexDB, db, precision, data, pointsCnt, try)
		}
	}
	//limit.Take()
	return writePointsv2(influxDb, db, precision, data, pointsCnt, try)
}

func writePointsv2(influxDb string, db string, precision string, data []byte, pointsCnt int, try bool) error {
	//defer limit.Release()
	fullURL := fmt.Sprintf("%s?%s", getURLv2(influxDb, "write"), ParseParams(map[string]string{
		"bucket":    db,
		"precision": "ns",
		"org":       config.GetConfig().TSDB.Org,
	}))

	// Set client timeout
	client := &http.Client{Timeout: time.Second * 5}
	req, err := newRequest(fullURL, data, config.GetConfig().TSDB.Token, "POST")
	if err != nil {
		return err
	}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		// clean cache, maybe config changed
		loda.PurgeChan <- db
		// clean cache
		mu.Lock()
		delete(v2Cache, db)
		mu.Unlock()
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// clean cache, maybe config changed
		loda.PurgeChan <- db
		return err
	} else if resp.StatusCode == 500 {
		return fmt.Errorf("Influxdb returned invalid status code: %v", resp.Status)
	} else if resp.StatusCode == 204 {

		// cache add
		if try {
			mu.Lock()
			v2Cache[db] = true
			mu.Unlock()
		}

		log.Debug(string(data))
		log.Infof("%d return by %s ,handle points %d", resp.StatusCode, influxDb, pointsCnt)
		return nil
	} else if (resp.StatusCode == 200 || resp.StatusCode == 404) && strings.Contains(string(body), "not found") {
		err := createDBAndRPv2(influxDb, db)
		if err != nil {
			return err
		}
		return fmt.Errorf("just create db, need retry the points")
	} else {
		log.Warningf("abandon points, unknow return from influxdb %s, status: %d, body: %s", influxDb, resp.StatusCode, resp.Body)
		return nil
	}
}

var rpMapv2 = map[string]int{
	".api.loda":     43200000,
	".switch.loda":  43200000,
	".mail.it.loda": 43200000,
}

func createDBAndRPv2(influxDB string, db string) (err error) {
	u := getURLv2(influxDB, "buckets")
	// {
	// 	"name": "string",
	// 	"orgID": "string",
	// 	"retentionRules": [
	// 	  {
	// 		"everySeconds": 7776000,
	// 		"type": "expire"
	// 	  }
	// 	],
	// 	"rp": "loda"
	// }
	type rr struct {
		EverySeconds int    `json:"everySeconds"`
		Type         string `json:"type"`
	}
	type bucket struct {
		Name           string `json:"name"`
		OrgID          string `json:"orgID"`
		RetentionRules []rr   `json:"retentionRules"`
		RP             string `json:"rp"`
	}
	// we keep 90 days by default, unit second.
	duration := 7776000
	for k, v := range rpMapv2 {
		if strings.HasSuffix(db, k) {
			duration = v
		}
	}

	b := bucket{
		Name:  db,
		OrgID: config.GetConfig().TSDB.Org,
		RetentionRules: []rr{{
			EverySeconds: duration,
			Type:         "expire",
		}},
		RP: "loda",
	}

	data, _ := json.Marshal(b)
	// Set client timeout
	client := &http.Client{Timeout: time.Second * 5}
	req, err := newRequest(u, data, config.GetConfig().TSDB.Token, "POST")
	if err != nil {
		return err
	}

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Influxdb returned invalid status code: %v", resp.Status)
	}
	return nil
}

func convLinePointv2(points []*models.Point) []string {
	var linePoints []string
	for _, point := range points {
		line, err := convLinev2(point)
		if err != nil {
			log.Warningf("point %v conv to line failed %s", point, err)
			continue
		}
		linePoints = append(linePoints, line)
	}
	return linePoints
}

func convLinev2(p *models.Point) (string, error) {
	key := p.Measurement

	if len(p.Tags) > 0 {
		var tags []string
		for k, v := range p.Tags {
			if "" == v {
				return "", fmt.Errorf("invalid tag value for %s", k)
			}
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
		}
		key = fmt.Sprintf("%s,%s", key, strings.Join(tags, ","))
	}

	var values []string
	for k, v := range p.Fields {
		if v == nil {
			return "", fmt.Errorf("invalid field value nil")
		}
		values = append(values, fmt.Sprintf("%s=%v", k, v))
	}

	value := strings.Join(values, ",")

	return fmt.Sprintf("%s %s %d", key, value, p.Timestamp*1e9), nil
}

func newRequest(url string, data []byte, token string, method string) (*http.Request, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Token "+token)
	return req, nil
}
