package gobinlog

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"banyan_api"

	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/client"
)

//Consumer 定义Consumer的结构
type Consumer struct {
	c *Config

	exitChan chan struct{}

	posLock sync.Mutex

	waitGroup WaitGroupWrapper
	poolSize  int

	IsRestart bool
	cluster   *banyan_api.ClusterClient
	client    *banyan_api.BanyanClient
	sqlcfg    MysqlConfig
	schemaMap map[string]string
}

//NewConsumer 初始化
func NewConsumer(c *Config, mysqlcfg MysqlConfig) (*Consumer, error) {
	//日志目录确保存在
	dir := filepath.Dir(c.LogConfig.Path)
	exist, _ := PathExists(dir)

	if !exist {
		err := os.Mkdir(dir, os.ModePerm)

		if err != nil {
			return nil, err
		}
	}

	//配置日志
	log.SetHighlighting(c.LogConfig.Highlighting)
	log.SetLevel(log.StringToLogLevel(c.LogConfig.Level))
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	log.SetOutputByName(c.LogConfig.Path)

	if c.LogConfig.Type == LogTypeDay {
		log.SetRotateByDay()
	} else if c.LogConfig.Type == LogTypeHour {
		log.SetRotateByHour()
	}

	consumer := &Consumer{
		c:         c,
		exitChan:  make(chan struct{}),
		IsRestart: false,
		schemaMap: make(map[string]string, 0),
	}
	var err error = nil
	log.Infof("ns:%s,table:%s", mysqlcfg.NsName, mysqlcfg.TableName)
	clusters := make([]string, 0)
	for _, v := range c.ClusterConfig.Agents {
		clusters = append(clusters, v)
	}
	consumer.cluster = banyan_api.NewClusterClient(clusters)

	consumer.client, err = consumer.cluster.GetBanyanClient(mysqlcfg.NsName, mysqlcfg.TableName, 3000, 3)
	if err != nil {
		log.Errorf("GetBanyanClient failed: %v", err)
		return nil, err
	}

	consumer.sqlcfg = mysqlcfg

	consumer.waitGroup.Wrap(func() { consumer.scanSchemaLoop() })

	return consumer, nil

}

//Close 关闭Consumer,释放资源
func (consumer *Consumer) Close() {

	close(consumer.exitChan)

	consumer.waitGroup.Wait()

	log.Infof("Consumer safe close. id = %s", consumer.sqlcfg.Id)
}

func (consumer *Consumer) scanSchemaLoop() {
	ticker := time.NewTicker(consumer.c.BinlogFlushMs * time.Millisecond)
	for {
		log.Infof("map = %s", consumer.schemaMap)
		select {
		case <-ticker.C:
			list, err := consumer.client.Qlist("", "", 10000)
			if err != nil {
				log.Errorf("get Qlist err")
				consumer.Close()
			}
			//log.Infof("list = %s", list)
			for _, v := range list {
				_, ok := consumer.schemaMap[v]
				//如果 ok 是 true, 则存在，否则不存在 /
				if !ok {
					consumer.schemaMap[v] = "ok"
					go consumer.processingSQL(v)
				}
			}

		case <-consumer.exitChan:
			log.Infof("save binlog position loop exit.")
			return
		}
	}

}

func (consumer *Consumer) processingSQL(schema string) {
	client1, err := consumer.cluster.GetBanyanClient(consumer.sqlcfg.NsName, consumer.sqlcfg.TableName, 3000, 3)
	if err != nil {
		log.Errorf("GetBanyanClient failed: %v", err)
		return
	}
	conn, err := client.Connect(consumer.sqlcfg.Addr, consumer.sqlcfg.User, consumer.sqlcfg.Password, "")
	if err != nil {
		log.Errorf("connect mysql server failed: %v conn: %s", err, conn)
		return
	}
	if schema != "common" {
		err = conn.UseDB(schema)
		if err != nil {
			log.Errorf("use schema error: %v ", err)
			return
		}
	}
	log.Infof("schema = %s", schema)
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			qsize, _ := client1.Qsize(schema)
			if qsize == 0 {
				time.Sleep(2)
				continue
			}
			sql, _ := client1.Qget(schema)
			log.Debugf("%s", sql)
			if sql != "" {
				_, err = conn.Execute(sql)
				if err == nil {
					sql, _ = client1.Qpop(schema) //执行成功，pop删除
					log.Infof("sql success :%s", sql)
				} else {
					log.Errorf("schema = %s isExecute error: %v sql: %s", schema, err, sql)
					return
				}
			}

		case <-consumer.exitChan:
			log.Infof("save binlog position loop exit.")
			return
		}
	}

}
