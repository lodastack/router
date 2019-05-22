package influx

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/lodastack/router/config"
	"github.com/lodastack/router/loda"
	"github.com/lodastack/router/models"
	"github.com/lodastack/router/requests"

	"github.com/lodastack/log"
)

var limit Fixed

const defaultWorkerNum = 10000

func init() {
	limit = NewFixed(defaultWorkerNum)
}

// Regular expression to match intranet IP Address
// include: 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16
const REGIntrannetIP = `^((192\.168|172\.([1][6-9]|[2]\d|3[01]))(\.([2][0-4]\d|[2][5][0-5]|[01]?\d?\d)){2}|10(\.([2][0-4]\d|[2][5][0-5]|[01]?\d?\d)){3})$`

type Tags struct {
	Columns []string      `json:"columns"`
	Values  []interface{} `json:"values"`
}

type ResultsObj struct {
	Results []Result `json:"results"`
}

type Result struct {
	Series []SeriesObj `json:"series, omitempty"`
}

type SeriesObj struct {
	Name    string        `json:"name"`
	Columns []string      `json:"columns"`
	Values  []interface{} `json:"values"`
}

func GetQueryUrl(host string) string {
	return fmt.Sprintf("http://%s:%d/query?", IntranetIP(host), config.GetConfig().Com.InfluxdPort)
}

func GetWriteUrl(host string) string {
	return fmt.Sprintf("http://%s:%d/write", IntranetIP(host), config.GetConfig().Com.InfluxdPort)
}

func IntranetIP(ipStr string) string {
	ips := strings.Split(ipStr, ",")
	if len(ips) == 1 {
		return ipStr
	}
	for _, ip := range ips {
		matched, _ := regexp.MatchString(REGIntrannetIP, ip)
		if matched {
			return ip
		}
	}
	return ips[0]
}

func ParseParams(params map[string]string) string {
	var urls []string
	for k, v := range params {
		param := fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
		urls = append(urls, param)
	}
	return strings.Join(urls, "&")
}

func Query(hosts []string, params map[string]string, ip string) (*ResultsObj, error) {
	resp, err := QueryRaw(hosts, params, ip)
	if err != nil {
		return nil, err
	}

	log.Debugf("Query influxdbs %v\n%s\nstatus:%d", hosts, params, resp.Status)

	rs := ResultsObj{}
	err = resp.Obj(&rs)
	if err != nil {
		log.Errorf("Error while read:\n%s\n%s\n", err.Error(), string(resp.Body))
		return nil, err
	}
	return &rs, nil
}

func QueryRaw(hosts []string, params map[string]string, ip string) (*requests.Resp, error) {

	var host string
	var resp *requests.Resp
	var err error

	if len(hosts) > 0 {
		host = hosts[0]
	} else {
		return resp, fmt.Errorf("no db config")
	}

	fullUrl := fmt.Sprintf("%s%s", GetQueryUrl(host), ParseParams(params))
	log.Infof("query [%s] ip [%s]", fullUrl, ip)

	resp, err = requests.Get(fullUrl)
	if err != nil {
		return resp, err
	}

	if resp.Status/100 != 2 {
		return resp, fmt.Errorf("Influxdb returned invalid status code: %v", resp.Status)
	}
	return resp, nil
}

func WritePoints(influxDbs []string, pointsObj models.Points) error {

	db := pointsObj.Database
	precision := "n"

	pointsCnt := len(pointsObj.Points)
	points := convLinePoint(pointsObj.Points)
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
			limit.Take()
			go writePoints(indexDB, db, precision, data, pointsCnt, pointsObj)
		}
	}
	limit.Take()
	return writePoints(influxDb, db, precision, data, pointsCnt, pointsObj)
}

var v2Cache = make(map[string]bool)
var mu sync.RWMutex

func writePoints(influxDb string, db string, precision string, data []byte, pointsCnt int, pointsObj models.Points) error {
	defer limit.Release()

	mu.RLock()
	_, ok := v2Cache[influxDb]
	mu.RUnlock()
	if ok || influxDb == "10.90.84.156" {
		return WritePointsv2([]string{influxDb}, pointsObj, false)
	}
	fullURL := fmt.Sprintf("%s?%s", GetWriteUrl(influxDb), ParseParams(map[string]string{
		"db":        db,
		"precision": precision,
	}))

	var err error
	var resp *requests.Resp
	if resp, err = requests.PostBytes(fullURL, data); err != nil {
		// clean cache, maybe config changed
		loda.PurgeChan <- db
		return WritePointsv2([]string{influxDb}, pointsObj, true)
		//return err
	} else if resp.Status == 500 {
		return fmt.Errorf("Influxdb returned invalid status code: %v", resp.Status)
	} else if resp.Status == 204 {
		log.Debug(string(data))
		log.Infof("%d return by %s ,handle points %d", resp.Status, influxDb, pointsCnt)
		return nil
	} else if (resp.Status == 200 || resp.Status == 404) && strings.Contains(string(resp.Body), "database not found") {
		err := createDBAndRP([]string{influxDb}, db)
		if err != nil {
			return err
		}
		return fmt.Errorf("just create db, need retry the points")
	} else {
		log.Warningf("abandon points, unknow return from influxdb %s, status: %d, body: %s", influxDb, resp.Status, resp.Body)
		return nil
	}
}

var rpMap = map[string]string{
	".api.loda":     "500d",
	".switch.loda":  "500d",
	".mail.it.loda": "500d",
}

func createDBAndRP(influxDbs []string, db string) (err error) {
	_, err = Query(influxDbs, map[string]string{
		"q": fmt.Sprintf("create database \"%s\"", db),
	}, "")

	if err != nil {
		log.Errorf("create database %s failed: %s", db, err)
		return err
	}

	rpd := "90d"
	for k, v := range rpMap {
		if strings.HasSuffix(db, k) {
			rpd = v
		}
	}

	_, err = Query(influxDbs, map[string]string{
		"q": fmt.Sprintf("CREATE RETENTION POLICY loda ON \"%s\" DURATION %s REPLICATION 1 DEFAULT", db, rpd),
	}, "")
	if err != nil {
		log.Errorf("create rp on db %s failed: %s", db, err)
		return err
	}

	return nil
}

func convLinePoint(points []*models.Point) []string {
	var linePoints []string
	for _, point := range points {
		line, err := convLine(point)
		if err != nil {
			log.Warningf("point %v conv to line failed %s", point, err)
			continue
		}
		linePoints = append(linePoints, line)
	}
	return linePoints
}

func convLine(p *models.Point) (string, error) {
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
