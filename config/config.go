package config

import (
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bitly/go-nsq"
	"github.com/lodastack/log"
)

var (
	mux        = new(sync.RWMutex)
	config     = new(Config)
	configPath = ""
)

type Config struct {
	Com       CommonConfig   `toml:"common"`
	Reg       RegistryConfig `toml:"registry"`
	Usg       UsageConfig    `toml:"usage"`
	LinkStats LinkStasConfig `toml:"linkstats"`
	IDC       []IDCConfig    `toml:"idc"`
	Nsq       NsqConfig      `toml:"nsq"`
	Log       LogConfig      `toml:"log"`
}

type CommonConfig struct {
	Listen              string `toml:"listen"`
	InfluxdPort         int    `toml:"influxdPort"`
	TopicsPollInterval  int    `toml:"topicsPollInterval"`
	HiddenMetricSuffix  string `toml:"hiddenMetricSuffix"`
	DBNS                string `toml:"DBNS"`
	DefaultDBCluster    string `toml:"defaultDBCluster"`
	DefaultAPINameSpace string `toml:"defaultAPINameSpace"`
}

type LogConfig struct {
	Enable   bool   `toml:"enable"`
	Path     string `toml:"path"`
	Level    string `toml:"level"`
	FileNum  int    `toml:"file_num"`
	FileSize int    `toml:"file_size"`
}

type RegistryConfig struct {
	Link      string `toml:"link"`
	ExpireDur int    `toml:"expireDur"`
}

type UsageConfig struct {
	Enable bool `toml:"enable"`
}

type LinkStasConfig struct {
	NS []string `toml:"ns"`
}

type IDCConfig struct {
	Name  string   `toml:"name"`
	Hosts []string `toml:"hosts"`
}

type NsqConfig struct {
	Enable              bool     `toml:"enable"`
	MaxAttempts         uint16   `toml:"maxAttempts"`
	MaxInFlight         int      `toml:"maxInFlight"`
	HeartbeatInterval   int      `toml:"heartbeatInterval"`
	ReadTimeout         int      `toml:"readTimeout"`
	LookupdPollInterval int      `toml:"lookupdPollInterval"`
	HandlerCount        int      `toml:"handlerCount"`
	Lookupds            []string `toml:"lookupds"`
	Chan                string   `toml:"chan"`
	TopicPrefix         string   `toml:"topicPrefix"`
}

func (this NsqConfig) GetNsqConfig() *nsq.Config {
	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxAttempts = this.MaxAttempts
	nsqConfig.MaxInFlight = this.MaxInFlight
	nsqConfig.HeartbeatInterval = time.Duration(this.HeartbeatInterval) * time.Millisecond
	nsqConfig.ReadTimeout = time.Duration(this.ReadTimeout) * time.Millisecond
	nsqConfig.LookupdPollInterval = time.Duration(this.LookupdPollInterval) * time.Millisecond

	return nsqConfig
}

func Reload() {
	err := LoadConfig(configPath)
	if err != nil {
		os.Exit(1)
	}
}

func LoadConfig(path string) (err error) {
	mux.Lock()
	defer mux.Unlock()
	configPath = path
	configFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Errorf("Error while loading config %s.\n%s\n", path, err.Error())
		return
	}
	if _, err = toml.Decode(string(configFile), &config); err != nil {
		log.Errorf("Error while decode the config %s.\n%s\n", path, err.Error())
		return
	} else {
		return nil
	}
}

func GetConfig() *Config {
	mux.RLock()
	defer mux.RUnlock()
	return config
}
