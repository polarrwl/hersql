package ntunnel

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
)

// TODO
func Query(query string) (result *sqltypes.Result, err error) {
	switch query {
	case "USE `UserDB`":
		result = &sqltypes.Result{}
	default:
		params := url.Values{}
		params.Set("actn", "Q")
		params.Set("q[]", query)
		params.Set("host", "")
		params.Set("port", "")
		params.Set("login", "")
		params.Set("password", "")
		params.Set("db", "UserDB")
		req, _ := http.NewRequest(http.MethodPost, "http://navicat.test.com:809/", strings.NewReader(params.Encode()))
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
	}

	return
}
