package navicat

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/Orlion/lakeman/pkg/bytesx"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"go.uber.org/zap"
)

type Reader struct {
	logger *zap.SugaredLogger
	body   io.ReadCloser
}

func NewReader(logger *zap.SugaredLogger, body io.ReadCloser) *Reader {
	return &Reader{logger, body}
}

func (r *Reader) Read() (result *sqltypes.Result, err error) {
	defer func() {
		fmt.Println(err, result)
	}()

	// read header
	errno, err := r.readHeader()
	if err != nil {
		return
	}

	if errno > 0 {

	} else {
		var errno, affectrows, insertid, numfields, numrows uint32
		errno, affectrows, insertid, numfields, numrows uint32, err = r.readResultSetHeader()
		if err != nil {
			return 
		}
	}

	fields := make([]*query.Field, 0)

	if numfields > 0 {
		// read fields header
		for i := 0; i < int(numfields); i++ {
			var fieldName []byte
			fieldName, err = readBlock(r.body)
			if err != nil {
				return
			}
			var fieldTable []byte
			fieldTable, err = readBlock(r.body)
			var fieldType int32
			fieldType, err = readInt32(r.body)
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
			fieldIntflag, err = readInt32(r.body)
			if err != nil {
				err = fmt.Errorf("read fieldIntflag error: [%w]", err)
				return
			}
			var fieldLength int32
			fieldLength, err = readInt32(r.body)
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
			b := readByte(r.body)
			var len int32
			var val []byte
			if b == '\xFF' {
				// 是null
			} else {
				if b == '\xFE' {
					len, err = readInt32(r.body)
					if err != nil {
						err = fmt.Errorf("read row field len error: [%w]", err)
						return
					}
				} else {
					len = int32(b)
				}

				val := make([]byte, len)
				n, err = r.body.Read(val)
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
				allbody, _ := ioutil.ReadAll(r.body)
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

func (r *Reader) readHeader() (errno uint32, err error) {
	buf := make([]byte, 6)
	n, err := r.body.Read(buf)
	if err != nil {
		err = fmt.Errorf("Reader.readHeader read head buf error: [%w]", err)
		return
	}
	if n != 6 {
		err = errors.New(fmt.Sprintf("Reader.readHeader read head buf n:%d != 6", n))
		return
	}

	errno, err = r.readUint32()
	if err != nil {
		err = fmt.Errorf("Reader.readHeader read errno error: [%w]", err)
		return
	}

	n, err = r.body.Read(buf)
	if err != nil {
		err = fmt.Errorf("Reader.readHeader read tail buf error: [%w]", err)
		return
	}
	if n != 6 {
		err = errors.New(fmt.Sprintf("Reader.readHeader read tail buf n:%d != 6", n))
		return
	}

	return
}

func (r *Reader) readResultSetHeader() (errno, affectrows, insertid, numfields, numrows uint32, err error) {
	errno, err = r.readUint32()
	if err != nil {
		err = fmt.Errorf("Reader.readResultSetHeader read errno error: [%w]", err)
		return
	}

	affectrows, err = r.readUint32()
	if err != nil {
		err = fmt.Errorf("Reader.readResultSetHeader read affectrows error: [%w]", err)
		return
	}

	insertid, err = r.readUint32()
	if err != nil {
		err = fmt.Errorf("Reader.readResultSetHeader read insertid error: [%w]", err)
		return
	}

	numfields, err = r.readUint32()
	if err != nil {
		err = fmt.Errorf("Reader.readResultSetHeader read numfields error: [%w]", err)
		return
	}

	numrows, err = r.readUint32()
	if err != nil {
		err = fmt.Errorf("Reader.readResultSetHeader read numrows error: [%w]", err)
		return
	}

	buf := make([]byte, 12)
	n, err := r.body.Read(buf)
	if err != nil {
		err = fmt.Errorf("Reader.readResultSetHeader read buf error: [%w]", err)
		return
	}
	if n != 12 {
		err = errors.New(fmt.Sprintf("Reader.readResultSetHeader read buf n:%d != 12", n))
		return
	}

	return
}

func (r *Reader) readUint32() (value uint32, err error) {
	valueBys := make([]byte, 4)
	n, err := r.body.Read(valueBys)
	if err != nil {
		return
	}
	if n != 4 {
		err = errors.New(fmt.Sprint("Reader.readUint32 read n:%d != 4", n))
		return
	}

	value = bytesx.Bytes2Uint32(valueBys)

	return
}

func readBlock(body io.ReadCloser) (value []byte, err error) {
	lenBys := make([]byte, 1)
	n, err := body.Read(lenBys)
	if err != nil {
		err = fmt.Errorf("readBlock lenBys err:[%w]", err)
		return
	}
	if n != 1 {
		err = errors.New(fmt.Sprintf("readBlock len's n:[%d] != 1", n))
		return
	}
	var len int32
	if lenBys[0] == '\xFE' {
		lenBys := make([]byte, 4)
		n, err = body.Read(lenBys)
		if err != nil {
			err = fmt.Errorf("readBlock lenBys2 err:[%w]", err)
			return
		}
		if n != 4 {
			return
		}

		len = bytesx.Bytes2Int32(lenBys)
	} else {
		len = int32(lenBys[0])
	}

	value = make([]byte, len)
	n, err = body.Read(value)
	if err != nil {
		err = fmt.Errorf("readBlock value err:[%w]", err)
		return
	}
	if n != int(len) {
		err = errors.New(fmt.Sprintf("readBlock values's n:[%d] != [%d]", n, len))
		return
	}

	return
}

// 读1byte
func readByte(body io.ReadCloser) (b byte) {
	bys := make([]byte, 1)
	n, err := body.Read(bys)
	if err != nil {
		return
	}
	if n != 1 {
		return
	}

	b = bys[0]

	return
}
