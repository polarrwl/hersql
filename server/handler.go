package server

import (
	"sync"
	"time"

	"github.com/Orlion/hersql/ntunnel"
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
	h.logger.Infof("NewConnection:[%s], id:[%d]", c.Conn.RemoteAddr().String(), c.ID)
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
) (err error) {
	defer func() {
		if err != nil {
			h.logger.Errorf("连接:[%s], id:[%d] query:[%s], err:[%s]", c.Conn.RemoteAddr().String(), c.ID, query, err.Error())
		} else {
			h.logger.Infof("连接:[%s], id:[%d] query:[%s], success", c.Conn.RemoteAddr().String(), c.ID, query)
		}
	}()

	result, err := ntunnel.Query(query)
	if err != nil {
		return
	}

	err = callback(result)

	return
}

func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}
