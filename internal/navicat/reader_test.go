package navicat

import (
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestRead(t *testing.T) {
	body1, err := os.Open("../../tests/navicat/body1.txt")
	if err != nil {
		t.Fatalf("body1 文件打开失败:[%s]", err.Error())
	}

	defer body1.Close()

	res, err := NewReader(body1).Read()
	if err != nil {
		t.Errorf("read body1 err:[%s]", err.Error())
	}

	t.Log(res)

	params := url.Values{}
	params.Set("actn", "Q")
	params.Set("q[]", "select * from address where id =12")
	params.Set("host", "")
	params.Set("port", "")
	params.Set("login", "")
	params.Set("password", "")
	params.Set("db", "UserDB")
	resp, err := http.Post("http://navicat.test.com:809/", "application/x-www-form-urlencoded", strings.NewReader(params.Encode()))
	if err != nil {
		t.Errorf("query err:[%s]", err.Error())
	} else {
		result, err := NewReader(resp.Body).Read()
		if err != nil {
			t.Errorf("query err2:[%s]", err.Error())
		}
		t.Log(result)
	}
}
