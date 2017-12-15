package gobinlog

import (
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type Config struct {
	BinlogFlushMs time.Duration  `toml:"binlog_flush_ms"`
	LogConfig     *LogConfig     `toml:"log"`
	MysqlConfig   *[]MysqlConfig `toml:"mysql"`
	ClusterConfig *ClusterConfig `toml:"clusters"`
}
type ClusterConfig struct {
	Agents []string `toml:"agents"`
}
type LogConfig struct {
	Path         string `toml:"log_path"`
	Type         int    `toml:"type"`
	Highlighting bool   `toml:"highlighting"`
	Level        string `toml:"level"`
}

type MysqlConfig struct {
	Id        string `toml:"id"`
	Addr      string `toml:"addr"`
	User      string `toml:"user"`
	Password  string `toml:"password"`
	Flavor    string `toml:"flavor"`
	DataPath  string `toml:"data_path"`
	QueueKey  string `toml:"queue"`
	NsName    string `toml:"nsname"`
	TableName string `toml:"tablename"`
}

//NewConfigWithFile 读取配置文件
func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

//NewConfig 解析配置文件
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
