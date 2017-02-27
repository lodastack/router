package worker

import (
	"fmt"
	golog "log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lodastack/router/config"
	"github.com/lodastack/router/requests"

	"github.com/lodastack/log"
)

type MasterWorker struct {
	Nsqlookupds []string
	Consumers   map[string]*NsqWorker
	topicsLock  *sync.RWMutex

	Channel   string
	NsqConfig NsqWorkerConfig

	TopicsPollInterval time.Duration
	exitChan           chan int
	reloadTopicsChan   chan int
}

func NewMaster() *MasterWorker {
	nsqConfig := NsqWorkerConfig{
		Config:      config.GetConfig().Nsq.GetNsqConfig(),
		ConCount:    config.GetConfig().Nsq.HandlerCount,
		Nsqlookupds: config.GetConfig().Nsq.Lookupds,
		Logger:      golog.New(os.Stdout, "nsq", golog.Ldate|golog.Ltime),
		LogLevel:    2,
	}

	return &MasterWorker{
		Nsqlookupds:        config.GetConfig().Nsq.Lookupds,
		Channel:            config.GetConfig().Nsq.Chan,
		TopicsPollInterval: time.Duration(config.GetConfig().Com.TopicsPollInterval) * time.Millisecond,
		NsqConfig:          nsqConfig,
		Consumers:          make(map[string]*NsqWorker),
		topicsLock:         new(sync.RWMutex),
		reloadTopicsChan:   make(chan int),
		exitChan:           make(chan int),
	}
}

func (this *MasterWorker) Start() {
	log.Info("Master loop started!")
	if !config.GetConfig().Nsq.Enable {
		log.Info("nsq consumer disabled")
		return
	}

	this.consumeAllTopics()
	var ticker *time.Ticker
	ticker = time.NewTicker(this.TopicsPollInterval)
	for {
		select {
		case <-ticker.C:
			this.consumeAllTopics()
		case <-this.reloadTopicsChan:
			this.consumeAllTopics()
		case <-this.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	log.Info("Master worker stoped!")
}

func (this *MasterWorker) Exit() {
	close(this.exitChan)
}

func (this *MasterWorker) ReloadTopics() {
	this.reloadTopicsChan <- 1
}

type Topics struct {
	StatusCode int       `json:"status_code"`
	StatusTxt  string    `json:"status_txt"`
	Data       TopicList `json:"data"`
}

type TopicList struct {
	Topics []string `json:"topics"`
}

func (this *MasterWorker) consumeAllTopics() {
	log.Info("Load topics!")
	for _, nsqlookupd := range this.Nsqlookupds {
		resp, err := requests.Get(fmt.Sprintf("http://%s/topics", nsqlookupd))
		if err != nil {
			log.Errorf("connect to %s nsqlookupd failed!\n%s\n", nsqlookupd, err.Error())
			continue
		}
		topics := new(Topics)
		err = resp.Obj(topics)
		if err != nil {
			log.Errorf("unmarshal resp from %s failed!\n%s\n", nsqlookupd, err.Error())
			continue
		}
		if topics.StatusCode != 200 {
			log.Errorf("return status code %d from %s!\n", topics.StatusCode, nsqlookupd)
			continue
		}
		for _, topic := range topics.Data.Topics {
			if strings.HasPrefix(topic, config.GetConfig().Nsq.TopicPrefix) {
				this.upConsumer(topic)
			}
		}
	}
}

func (this *MasterWorker) upConsumer(topic string) {
	this.topicsLock.RLock()
	w, ok := this.Consumers[topic]
	this.topicsLock.RUnlock()
	if ok && w != nil {
		return
	}

	nsqWorker, err := NewWorker(topic, config.GetConfig().Nsq.Chan, this.NsqConfig)
	if err != nil {
		log.Errorf("up consumer for %s failed.\n", topic)
		return
	}
	this.topicsLock.Lock()
	this.Consumers[topic] = nsqWorker
	this.topicsLock.Unlock()
}
