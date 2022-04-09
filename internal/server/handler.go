package server

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Orlion/lakeman/pkg/bytesx"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"go.uber.org/zap"
)

type Handler struct {
	mu          sync.Mutex
	readTimeout time.Duration
	logger      *zap.SugaredLogger
}

func newHandler(readTimeout time.Duration, logger *zap.SugaredLogger) *Handler {
	return &Handler{
		readTimeout: readTimeout,
		logger:      logger,
	}
}

func (h *Handler) NewConnection(c *mysql.Conn) {
	h.logger.Infof("接收到来自新连接:[%s], id:[%d]", c.Conn.RemoteAddr().String(), c.ID)
}

func (h *Handler) ComInitDB(c *mysql.Conn, schemaName string) error {
	h.logger.Infof("ComInitDB, [%s], id:[%d] schemaName:[%s]", c.Conn.RemoteAddr().String(), c.ID, schemaName)
	return nil
}

func (h *Handler) ComPrepare(c *mysql.Conn, query string) ([]*query.Field, error) {
	return nil, nil
}

func (h *Handler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (h *Handler) ComResetConnection(c *mysql.Conn) {
	// TODO: handle reset logic
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	h.logger.Infof("连接:[%s], id:[%d] 关闭", c.Conn.RemoteAddr().String(), c.ID)
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result) error,
) error {
	h.logger.Infof("连接:[%s], id:[%d] query:[%s]", c.Conn.RemoteAddr().String(), c.ID, query)

	switch query {
	case "USE ``":
		return callback(&sqltypes.Result{})
	default:
		params := url.Values{}
		params.Set("actn", "Q")
		params.Set("q[]", query)
		params.Set("host", "")
		params.Set("port", "")
		params.Set("login", "")
		params.Set("password", "")
		params.Set("db", "")
		resp, err := http.Post("http://navicat.test.com:809/", "application/x-www-form-urlencoded", strings.NewReader(params.Encode()))
		if err != nil {
			h.logger.Errorf("请求navicat api错误", err)
			return err
		} else {
			return callback(parseNavicatResp(resp))
		}
	}
}

func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (h *Handler) parseNavicatResp(resp *http.Response) (result *sqltypes.Result) {
	result = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 0,
		InsertID:     0,
		Info:         "",
		Rows:         nil,
		Extras:       nil,
	}

	// read header
	buf := make([]byte, 6)
	n, err := resp.Body.Read(buf)
	if err != nil {
		return
	}
	if n != 6 {
		return
	}

	errnoBys := make([]byte, 4)
	n, err = resp.Body.Read(errnoBys)
	if err != nil {
		return
	}
	if n != 4 {
		return
	}

	errno := bytesx.Bytes2Int32(errnoBys)
	if errno != 0 {
		return
	}

	tmp := make([]byte, 6)
	resp.Body.Read(tmp)

	// read result header
	errnoBys = make([]byte, 4)
	n, err = resp.Body.Read(errnoBys)
	if err != nil {
		return
	}
	if n != 4 {
		return
	}

	affectrowsBys := make([]byte, 4)
	n, err = resp.Body.Read(affectrowsBys)
	if err != nil {
		return
	}
	if n != 4 {
		return
	}

	insertidBys := make([]byte, 4)
	n, err = resp.Body.Read(insertidBys)
	if err != nil {
		return
	}
	if n != 4 {
		return
	}

	numfieldsBys := make([]byte, 4)
	n, err = resp.Body.Read(numfieldsBys)
	if err != nil {
		return
	}
	if n != 4 {
		return
	}

	numrowsBys := make([]byte, 4)
	n, err = resp.Body.Read(numrowsBys)
	if err != nil {
		return
	}
	if n != 4 {
		return
	}

	tmp = make([]byte, 6)
	resp.Body.Read(tmp)

	return
}
