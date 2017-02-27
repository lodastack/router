package worker

import (
	"encoding/json"
	golog "log"

	"github.com/lodastack/router/influx"
	"github.com/lodastack/router/loda"
	"github.com/lodastack/router/models"

	"github.com/bitly/go-nsq"
	"github.com/lodastack/log"
)

type NsqWorkerConfig struct {
	Config      *nsq.Config
	Logger      *golog.Logger
	LogLevel    nsq.LogLevel
	ConCount    int
	Nsqlookupds []string
}

type NsqWorker struct {
	Namespace string
	Channel   string
	Consumer  *nsq.Consumer
}

func NewWorker(topic string, channel string, config NsqWorkerConfig) (this *NsqWorker, err error) {

	this = &NsqWorker{
		Namespace: topic,
		Channel:   channel,
	}

	this.Consumer, err = nsq.NewConsumer(this.Namespace, this.Channel, config.Config)
	if err != nil {
		log.Errorf("Worker for %s error while set up new consumer.\n%s\n", this.Namespace, err.Error())
		return
	}

	this.Consumer.SetLogger(config.Logger, config.LogLevel)
	this.Consumer.AddConcurrentHandlers(this, config.ConCount)
	if err = this.Consumer.ConnectToNSQLookupds(config.Nsqlookupds); err != nil {
		log.Errorf("Worker for %s error while connect to nsqlookupds.\n%s\n", this.Namespace, err.Error())
		return
	}
	return
}

func (this *NsqWorker) HandleMessage(m *nsq.Message) error {
	originPointsObj := models.Points{}
	err := json.Unmarshal(m.Body, &originPointsObj)
	if err != nil {
		log.Warningf("invalid points body abandoned, %s, %s", m.Body, err)
		return nil
	}

	pointsObj := originPointsObj
	if len(pointsObj.Points) == 0 {
		return nil
	}

	influxdbs, err := loda.InfluxDBs(this.Namespace)
	if err != nil {
		return err
	}

	if len(influxdbs) == 0 {
		log.Warningf("get empty influxdbs config, ignore the points")
		return nil
	}

	if err := influx.WritePoints(influxdbs, pointsObj); err != nil {
		log.Errorf("<%s> post message to influxdbs %v failed: %s", this.Namespace, influxdbs, err.Error())
		return err
	}
	return nil
}
