package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	//_ "github.com/go-sql-driver/mysql"
	"goproducer/producer"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var (
	configFile = flag.String("config", "./conf/producer.toml", "go-rail config file")
	binlogName = flag.String("binlog_name", "", "binlog file name")
	binlogPos  = flag.Int64("binlog_pos", 0, "binlog position")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var err error
	var config *producer.Config
	config, err = producer.NewConfigWithFile(*configFile)
	if err != nil {
		log.Fatalf("config load failed.detail=%s", errors.ErrorStack(err))
	}
	for i, v := range *config.MysqlConfig {
		r, err := producer.NewRail(config, v)
		defer r.Close()

		if err != nil {
			fmt.Println("new Rail error.", err)
			log.Fatalf("new Rail error. detail:%v", err)
		}

		fmt.Println("rail start succ. %d", i)
	}

	signal := <-sc

	log.Errorf("program terminated! signal:%v", signal)

}
