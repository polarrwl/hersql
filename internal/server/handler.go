package server

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

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
	case "USE `UserDB`":
		return callback(&sqltypes.Result{})
	default:
		params := url.Values{}
		params.Set("actn", "Q")
		params.Set("q[]", query)
		params.Set("host", "")
		params.Set("port", "")
		params.Set("login", "")
		params.Set("password", "")
		params.Set("db", "UserDB")
		resp, err := http.Post("http://navicat.test.com:809/", "application/x-www-form-urlencoded", strings.NewReader(params.Encode()))
		if err != nil {
			h.logger.Errorf("请求navicat api错误", err)
			return err
		} else {
			result, err := h.parseNavicatResp(resp)
			if err != nil {
				return err
			}
			return callback(result)
		}
	}
}

func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (h *Handler) parseNavicatResp(resp *http.Response) (result *sqltypes.Result, err error) {
	defer func() {
		fmt.Println(err, result)
	}()

	body := resp.Body

	// read header
	header := make([]byte, 6)
	n, err := body.Read(header)
	if err != nil {
		err = fmt.Errorf("read header error: [%w]", err)
		return
	}
	if n != 6 {
		err = errors.New(fmt.Sprintf("read header len:[%d] != 6", n))
		return
	}

	errno, err := readInt32(body)
	if err != nil {
		err = fmt.Errorf("read errno error: [%w]", err)
		return
	}

	tmp := make([]byte, 6)
	body.Read(tmp)

	// read result header
	errno, err = readInt32(body)

	h.logger.Infof("errono: [%d]", errno)

	affectrows, err := readInt32(body)
	if err != nil {
		err = fmt.Errorf("read affectrows error: [%w]", err)
		return
	}

	insertid, err := readInt32(body)
	if err != nil {
		err = fmt.Errorf("read insertid error: [%w]", err)
		return
	}

	numfields, err := readInt32(body)
	if err != nil {
		err = fmt.Errorf("read numfields error: [%w]", err)
		return
	}

	numrows, err := readInt32(body)
	if err != nil {
		err = fmt.Errorf("read numrows error: [%w]", err)
		return
	}

	tmp = make([]byte, 6)
	body.Read(tmp)

	fields := make([]*query.Field, 0)

	if numfields > 0 {
		// read fields header
		for i := 0; i < int(numfields); i++ {
			var fieldName []byte
			fieldName, err = readBlock(body)
			if err != nil {
				return
			}
			var fieldTable []byte
			fieldTable, err = readBlock(body)
			var fieldType int32
			fieldType, err = readInt32(body)
			if err != nil {
				err = fmt.Errorf("read fieldType error: [%w]", err)
				return
			}
			if _, exists := query.Type_name[fieldType]; !exists {
				err = errors.New(fmt.Sprintf("read invalid field type: [%d]", fieldType))
				fmt.Println(err)
				os.Exit(1)
				return
			}
			var fieldIntflag int32
			fieldIntflag, err = readInt32(body)
			if err != nil {
				err = fmt.Errorf("read fieldIntflag error: [%w]", err)
				return
			}
			var fieldLength int32
			fieldLength, err = readInt32(body)
			if err != nil {
				err = fmt.Errorf("read fieldLength error: [%w]", err)
				return
			}
			fields = append(fields, &query.Field{
				Name:         string(fieldName),
				Table:        string(fieldTable),
				ColumnLength: uint32(fieldLength),
				Flags:        uint32(fieldIntflag),
				Type:         query.Type(fieldType),
			})
		}
	} else {

	}

	rows := make([][]sqltypes.Value, 0)

	// read data
	for i := 0; i < int(numrows); i++ {
		row := make([]sqltypes.Value, 0)
		for j := 0; j < int(numfields); j++ {
			// 先读一个字节判断是否是null
			b := readByte(body)
			var len int32
			var val []byte
			if b == '\xFF' {
				// 是null
			} else {
				if b == '\xFE' {
					len, err = readInt32(body)
					if err != nil {
						err = fmt.Errorf("read row field len error: [%w]", err)
						return
					}
				} else {
					len = int32(b)
				}

				val := make([]byte, len)
				n, err = body.Read(val)
				if err != nil {
					err = fmt.Errorf("read row field val error: [%w]", err)
					return
				}
				if n != int(len) {
					err = fmt.Errorf("read row field val len:[%d] != [%d]", n, len)
					return
				}
			}
			var sqlval sqltypes.Value
			sqlval, err = sqltypes.NewValue(fields[j].Type, val)
			if err != nil {
				err = fmt.Errorf("sqltypes.NewValue error: [%w]", err)
				allbody, _ := ioutil.ReadAll(resp.Body)
				fmt.Println(allbody, fields[j].Type, val)
				os.Exit(-1)
				return
			}
			row = append(row, sqlval)
		}

		rows = append(rows, row)
	}

	result = &sqltypes.Result{
		Fields:       fields,
		RowsAffected: uint64(affectrows),
		InsertID:     uint64(insertid),
		Info:         "",
		Rows:         rows,
		Extras:       nil,
	}

	return
}
