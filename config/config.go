package config

import (
	"errors"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

const (
	DefaultServerProtocol         = "tcp"
	DefaultServerAddress          = "127.0.0.1:3306"
	DefaultServerConnReadTimeout  = 5000
	DefaultServerConnWriteTimeout = 5000
	DefaultServerMaxConnections   = 10
	DefaultServerUserName         = "root"
)

type Conf struct {
	Server     *Server `yaml:"server"`
	Log        *Log    `yaml:"log"`
	NtunnelUrl string  `yaml:"ntunnel_url"`
}

type Server struct {
	Protocol         string `yaml:"protocol"`
	Address          string `yaml:"address"`
	Version          string `yaml:"version"`
	ConnReadTimeout  uint64 `yaml:"conn_read_timeout"`
	ConnWriteTimeout uint64 `yaml:"conn_write_timeout"`
	MaxConnections   uint64 `yaml:"max_connections"`
	UserName         string `yaml:"user_name"`
	UserPassword     string `yaml:"user_password"`
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

	if conf.NtunnelUrl == "" {
		err = errors.New("please specify ntunnel_url in the configuration file")
		return
	}

	withDefault(conf)

	return
}

//fill conf with default values
func withDefault(conf *Conf) {
	if conf.Server == nil {
		conf.Server = new(Server)
	}

	if conf.Server.Protocol == "" {
		conf.Server.Protocol = DefaultServerProtocol
	}
	if conf.Server.Address == "" {
		conf.Server.Address = DefaultServerAddress
	}
	if conf.Server.ConnReadTimeout == 0 {
		conf.Server.ConnReadTimeout = DefaultServerConnReadTimeout
	}
	if conf.Server.ConnWriteTimeout == 0 {
		conf.Server.ConnWriteTimeout = DefaultServerConnWriteTimeout
	}
	if conf.Server.MaxConnections == 0 {
		conf.Server.MaxConnections = DefaultServerMaxConnections
	}
	if conf.Server.UserName == "" {
		conf.Server.UserName = DefaultServerUserName
	}

	if conf.Log == nil {
		conf.Log = new(Log)
	}
}
