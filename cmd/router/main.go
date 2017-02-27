package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/lodastack/router/config"
	"github.com/lodastack/router/loda"
	"github.com/lodastack/router/query"
	"github.com/lodastack/router/worker"

	"github.com/lodastack/log"
)

func initLog(conf config.LogConfig) {
	if !conf.Enable {
		fmt.Println("log to std err")
		log.LogToStderr()
		return
	}

	if backend, err := log.NewFileBackend(conf.Path); err != nil {
		fmt.Fprintf(os.Stderr, "init logs folder failed: %s\n", err.Error())
		os.Exit(1)
	} else {
		log.SetLogging(conf.Level, backend)
		backend.Rotate(conf.FileNum, uint64(1024*1024*conf.FileSize))
	}
}

func init() {
	configFile := flag.String("c", "./conf/router.conf", "config file path")
	flag.Parse()
	fmt.Printf("load config from %s\n", *configFile)
	err := config.LoadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read config file failed:\n%s\n", err.Error())
		os.Exit(1)
	}
	initLog(config.GetConfig().Log)
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	fmt.Println("build via golang version ", runtime.Version())
	m := worker.NewMaster()
	go m.Start()
	go query.Start()
	go loda.PurgeAll()
	select {}
}
