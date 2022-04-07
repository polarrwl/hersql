package server

import (
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

type Handler struct {
	mu          sync.Mutex
	readTimeout time.Duration
}

func newHandler(readTimeout time.Duration) *Handler {
	return &Handler{
		readTimeout: readTimeout,
	}
}

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
}

func (h *Handler) ComInitDB(c *mysql.Conn, schemaName string) error {
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
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result) error,
) error {
	return nil
}

func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}
