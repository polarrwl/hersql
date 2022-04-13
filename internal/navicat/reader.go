package navicat

import (
	"errors"
	"fmt"
	"io"

	"github.com/Orlion/lakeman/pkg/bytesx"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

type Reader struct {
	body io.ReadCloser
}

func NewReader(body io.ReadCloser) *Reader {
	return &Reader{body}
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
		errno, affectrows, insertid, numfields, numrows, err = r.readResultSetHeader()
		if err != nil {
			return
		}

		if errno > 0 {
			var mysqlierr []byte
			mysqlierr, err = r.readBlockValue()
			if err != nil {
				return
			}

			result = &sqltypes.Result{
				Info: string(mysqlierr),
			}
		} else {
			if numfields > 0 {
				var fields []*query.Field
				fields, err = r.readFieldsHeader(numfields)
				if err != nil {
					return
				}

				var rows [][]sqltypes.Value
				rows, err = r.readData(fields, numrows, numfields)
				if err != nil {
					return
				}

				result = &sqltypes.Result{
					Fields:       fields,
					RowsAffected: uint64(affectrows),
					InsertID:     uint64(insertid),
					Rows:         rows,
				}
			} else {
				var info []byte
				info, err = r.readBlockValue()
				if err != nil {
					return
				}

				result = &sqltypes.Result{
					Info: string(info),
				}
			}
		}
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

func (r *Reader) readFieldsHeader(numfields uint32) (fields []*query.Field, err error) {
	fields = make([]*query.Field, numfields)

	var i uint32
	for ; i < numfields; i++ {
		var fieldName []byte
		fieldName, err = r.readBlockValue()
		if err != nil {
			return
		}

		var fieldTable []byte
		fieldTable, err = r.readBlockValue()

		var fieldType uint32
		fieldType, err = r.readUint32()
		if err != nil {
			return
		}

		var fieldIntflag uint32
		fieldIntflag, err = r.readUint32()
		if err != nil {
			return
		}

		var fieldLength uint32
		fieldLength, err = r.readUint32()
		if err != nil {
			return
		}

		var queryFieldType query.Type
		queryFieldType, err = sqltypes.MySQLToType(int64(fieldType), int64(fieldIntflag))
		if err != nil {
			err = fmt.Errorf("Reader.readFieldsHeader sqltypes.MySQLToType err:[%w]", err)
			return
		}

		fields[i] = &query.Field{
			Name:         string(fieldName),
			Table:        string(fieldTable),
			ColumnLength: uint32(fieldLength),
			Flags:        uint32(fieldIntflag),
			Type:         queryFieldType,
		}
	}

	return
}

func (r *Reader) readData(fields []*query.Field, numrows, numfields uint32) (rows [][]sqltypes.Value, err error) {
	rows = make([][]sqltypes.Value, numrows)

	var i uint32
	for ; i < numrows; i++ {
		row := make([]sqltypes.Value, numfields)

		var j uint32
		for j = 0; j < numfields; j++ {
			// 先读一个字节判断是否是null
			var b byte
			b, err = r.readByte()
			if err != nil {
				return
			}

			var val []byte
			if b == '\xFF' {
				// 是null
			} else {
				val, err = r.readBlockValueWithFirstByte(b)
				if err != nil {
					return
				}
			}

			var sqlval sqltypes.Value
			sqlval, err = sqltypes.NewValue(fields[j].Type, val)
			if err != nil {
				err = fmt.Errorf("Reader.readData sqltypes.NewValue error: [%w]", err)
				return
			}
			row[j] = sqlval
		}

		rows[i] = row
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

func (r *Reader) readBlockValue() (value []byte, err error) {
	lenBys := make([]byte, 1)
	n, err := r.body.Read(lenBys)
	if err != nil {
		err = fmt.Errorf("Reader.readBlockValue read lenBys err:[%w]", err)
		return
	}
	if n != 1 {
		err = errors.New(fmt.Sprintf("Reader.readBlockValue read lenBys n:[%d] != 1", n))
		return
	}

	var len uint32
	if lenBys[0] == '\xFE' {
		len, err = r.readUint32()
		if err != nil {
			return
		}
	} else {
		len = uint32(lenBys[0])
	}

	value = make([]byte, len)
	n, err = r.body.Read(value)
	if err != nil {
		err = fmt.Errorf("Reader.readBlockValue read value err:[%w]", err)
		return
	}
	if n != int(len) {
		err = errors.New(fmt.Sprintf("Reader.readBlockValue read value n:[%d] != [%d]", n, len))
		return
	}

	return
}

func (r *Reader) readBlockValueWithFirstByte(b byte) (value []byte, err error) {
	var len uint32
	if b == '\xFE' {
		len, err = r.readUint32()
		if err != nil {
			return
		}
	} else {
		len = uint32(b)
	}

	value = make([]byte, len)
	n, err := r.body.Read(value)
	if err != nil {
		err = fmt.Errorf("Reader.readBlockValue read value err:[%w]", err)
		return
	}
	if n != int(len) {
		err = errors.New(fmt.Sprintf("Reader.readBlockValue read value n:[%d] != [%d]", n, len))
		return
	}

	return
}

// 读1byte
func (r *Reader) readByte() (b byte, err error) {
	bys := make([]byte, 1)
	n, err := r.body.Read(bys)
	if err != nil {
		return
	}
	if n != 1 {
		return
	}

	b = bys[0]

	return
}
