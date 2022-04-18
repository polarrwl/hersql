package ntunnel

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/go-sql-driver/mysql"
)

type DSN struct {
	Host     string
	Port     string
	Login    string
	Password string
	DB       string
}

func NewDSN(cfg *mysql.Config) (*DSN, error) {
	addrs := strings.Split(cfg.Addr, ":")
	if len(addrs) != 2 {
		return nil, errors.New("the addr must be in host:port format")
	}

	port, err := strconv.Atoi(addrs[1])
	if err != nil {
		return nil, fmt.Errorf("port:[%s] parse err:[%w]", addrs[1], err)
	}

	if port < 0 || port > math.MaxUint16 {
		return nil, fmt.Errorf("invalid port:[%d]", port)
	}

	return &DSN{
		Host:     addrs[0],
		Port:     addrs[1],
		Login:    cfg.User,
		Password: cfg.Passwd,
		DB:       cfg.DBName,
	}, nil
}

func (dsn *DSN) SetDB(db string) {
	dsn.DB = db
}

type Querier struct {
	ntunnelUrl string
}

func NewQuerier(ntunnelUrl string) *Querier {
	return &Querier{
		ntunnelUrl: ntunnelUrl,
	}
}

func (qer *Querier) Query(query string, dsn *DSN) (result *sqltypes.Result, err error) {
	params := url.Values{}
	params.Set("actn", "Q")
	params.Set("q[]", query)
	params.Set("host", dsn.Host)
	params.Set("port", dsn.Port)
	params.Set("login", dsn.Login)
	params.Set("password", dsn.Password)
	params.Set("db", dsn.DB)
	req, _ := http.NewRequest(http.MethodPost, qer.ntunnelUrl, strings.NewReader(params.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("accept-encoding", "identity")
	httpClient := &http.Client{
		Timeout: 5000 * time.Millisecond,
	}

	var resp *http.Response
	resp, err = httpClient.Do(req)
	if err != nil {
		return
	} else {
		result, err = NewParser(resp.Body).Parse()
		if err != nil {
			return
		}
	}

	return
}
