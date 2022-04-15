package config

import (
	"io/ioutil"
	"time"

	yaml "gopkg.in/yaml.v2"
)

type Conf struct {
	Server     *Server `yaml:"server"`
	Log        *Log    `yaml:"log"`
	NtunnelUrl string  `yaml:"ntunnel_url"`
}

type Server struct {
	Protocol         string        `yaml:"protocol"`
	Address          string        `yaml:"address"`
	Version          string        `yaml:"version"`
	ConnReadTimeout  time.Duration `yaml:"conn_read_timeout"`
	ConnWriteTimeout time.Duration `yaml:"conn_write_timeout"`
	MaxConnections   uint64        `yaml:"max_connections"`
	AccountName      string        `yaml:"user_name"`
	AccountPassword  string        `yaml:"user_password"`
}

type Log struct {
	InfoLogFilename  string `yaml:"info_log_filename"`
	ErrorLogFilename string `yaml:"error_log_filename"`
}

func Parse(filename string) (conf *Conf, err error) {
	fileData, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	conf = new(Conf)
	err = yaml.Unmarshal(fileData, conf)
	if err != nil {
		return
	}

	return
}
