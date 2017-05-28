package loda

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lodastack/router/requests"

	"github.com/lodastack/log"
)

const CommonCluster = "common"
const DefaultDBNameSpace = "db.monitor.loda"
const MachineUri = "/api/v1/router/resource?ns=%s&type=machine"
const CollectUri = "/api/v1/router/resource?ns=%s&type=collect"

var (
	PurgeChan    chan string
	Client       *client
	RegistryAddr string
	ExpireDur    int
)

type client struct {
	// cache ns -> dbs in this map
	db map[string][]string
	mu sync.RWMutex
}

type RespNS struct {
	Status int      `json:"httpstatus"`
	Data   []string `json:"data"`
}

type RespDB struct {
	Status int      `json:"httpstatus"`
	Data   []Server `json:"data"`
}

type Server struct {
	IP       string `json:"ip"`
	Hostname string `json:"hostname"`
}

func Init(regAddr string, expireDur int) {
	RegistryAddr = regAddr
	ExpireDur = expireDur
	PurgeChan = make(chan string)
	Client = &client{
		db: make(map[string][]string),
	}
}

func PurgeAll() {
	var ticker *time.Ticker
	interval := ExpireDur
	if interval < 60 {
		interval = 60
	}
	duration := time.Duration(interval) * time.Second
	ticker = time.NewTicker(duration)
	for {
		select {
		case <-ticker.C:
			url := fmt.Sprintf("%s/api/v1/router/ns?ns=&format=list", RegistryAddr)
			res, err := allNS(url)
			if err == nil {
				for _, ns := range res {
					dbs, err := updateInfluxDBs(ns)
					if err == nil {
						Client.mu.Lock()
						Client.db["collect."+ns] = dbs
						Client.mu.Unlock()
					} else {
						log.Errorf("update ns: %s cache failed: %s", ns, err)
					}
				}
				Client.mu.RLock()
				log.Infof("DB new cache: %v", Client.db)
				Client.mu.RUnlock()
			} else {
				log.Errorf("Get all NS failed: %s", err)
			}
		case ns := <-PurgeChan:
			Client.purge(ns)
		}
	}
}

func (c *client) purge(ns string) {
	c.mu.Lock()
	if _, ok := c.db[ns]; ok {
		delete(c.db, ns)
	}
	c.mu.Unlock()
	log.Infof("purge cache ns:%s", ns)
}

func InfluxDBs(ns string) ([]string, error) {
	var res []string
	var ok bool
	Client.mu.RLock()
	if res, ok = Client.db[ns]; ok {
		Client.mu.RUnlock()
		return res, nil
	}
	Client.mu.RUnlock()
	dbs, err := updateInfluxDBs(ns)
	if err != nil {
		return res, err
	}
	Client.mu.Lock()
	Client.db[ns] = dbs
	Client.mu.Unlock()
	return dbs, nil
}

func updateInfluxDBs(ns string) ([]string, error) {
	list := strings.Split(ns, ".")
	if len(list)-2 < 0 {
		return []string{}, fmt.Errorf("ns error: %s", ns)
	}
	partone := list[len(list)-2]
	uri := fmt.Sprintf(MachineUri, partone+"."+DefaultDBNameSpace)
	url := fmt.Sprintf("%s%s", RegistryAddr, uri)
	res, err := servers(url)
	if err != nil || len(res) > 0 {
		return res, err
	}

	url = fmt.Sprintf("%s/api/v1/router/ns?ns=%s&format=list", RegistryAddr, DefaultDBNameSpace)
	res, err = allNS(url)
	if err == nil {
		ok, cluster := includeNS(partone, res)
		if ok {
			uri = fmt.Sprintf(MachineUri, cluster+"."+DefaultDBNameSpace)
			url = fmt.Sprintf("%s%s", RegistryAddr, uri)
			res, err = servers(url)
			if err != nil || len(res) > 0 {
				return res, err
			}
		}
	} else {
		log.Errorf("get default DB NameSpace failed: %s", err)
	}

	// Send to common cluster if not found customer cluster
	uri = fmt.Sprintf(MachineUri, CommonCluster+"."+DefaultDBNameSpace)
	url = fmt.Sprintf("%s%s", RegistryAddr, uri)
	res, err = servers(url)
	if err != nil || len(res) > 0 {
		return res, err
	}

	return res, fmt.Errorf("common cluster status != 200")
}

func servers(url string) ([]string, error) {
	var res []string
	var resdb RespDB

	resp, err := requests.Get(url)
	if err != nil {
		return res, err
	}

	if resp.Status == 200 {
		err = json.Unmarshal(resp.Body, &resdb)
		if err != nil {
			return res, err
		}
		for _, s := range resdb.Data {
			res = append(res, s.IP)
		}
		return res, nil
	}
	// len(res) == 0
	return res, nil
}

func allNS(url string) ([]string, error) {
	var resNS RespNS
	var res []string
	resp, err := requests.Get(url)
	if err != nil {
		return res, err
	}

	if resp.Status == 200 {
		err = json.Unmarshal(resp.Body, &resNS)
		if err != nil {
			return res, err
		}
		return resNS.Data, nil
	}
	return res, fmt.Errorf("http status code: %d", resp.Status)
}

func includeNS(nsPartOne string, dbs []string) (bool, string) {
	for _, dbns := range dbs {
		parts := strings.Split(dbns, ".")
		// use "||" splite mutile NS part one
		nodes := strings.Split(parts[0], "||")
		for _, name := range nodes {
			if name == nsPartOne {
				return true, parts[0]
			}
		}
	}
	return false, ""
}

type CollectMetric struct {
	Name     string `json:"name"`
	Interval string `json:"interval"`
}

type RespCollect struct {
	Status int             `json:"httpstatus"`
	Data   []CollectMetric `json:"data"`
}

func CollectMetrics(ns string) ([]CollectMetric, error) {
	var res []CollectMetric
	var data RespCollect

	// remove "collect." from NS
	ns = strings.TrimPrefix(ns, "collect.")

	uri := fmt.Sprintf(CollectUri, ns)
	url := fmt.Sprintf("%s%s", RegistryAddr, uri)
	resp, err := requests.Get(url)
	if err != nil {
		return res, err
	}

	if resp.Status == 200 {
		err := json.Unmarshal(resp.Body, &data)
		if err != nil {
			return res, err
		}
		return data.Data, nil
	}
	return res, fmt.Errorf("get collect %s failed: status:%d", ns, resp.Status)
}
