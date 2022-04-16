package ntunnel

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
)

type Querier struct {
	ntunnelUrl string
}

func NewQuerier(ntunnelUrl string) *Querier {
	return &Querier{
		ntunnelUrl: ntunnelUrl,
	}
}

// TODO
func (qer *Querier) Query(query string) (result *sqltypes.Result, err error) {
	params := url.Values{}
	params.Set("actn", "Q")
	params.Set("q[]", query)
	params.Set("host", "")
	params.Set("port", "")
	params.Set("login", "")
	params.Set("password", "")
	params.Set("db", "UserDB")
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
