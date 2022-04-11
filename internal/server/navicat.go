package server

import (
	"errors"
	"fmt"
	"io"

	"github.com/Orlion/lakeman/pkg/bytesx"
)

func readInt32(body io.ReadCloser) (value int32, err error) {
	valueBys := make([]byte, 4)
	n, err := body.Read(valueBys)
	if err != nil {
		return
	}
	if n != 4 {
		err = errors.New(fmt.Sprint("readInt32 ren len:%d < 4", n))
		return
	}

	value = bytesx.Bytes2Int32(valueBys)

	return
}

func readBlock(body io.ReadCloser) (value []byte) {
	lenBys := make([]byte, 1)
	n, err := body.Read(lenBys)
	if err != nil {
		return
	}
	if n != 1 {
		return
	}
	var len int32
	if lenBys[0] == '\xFE' {
		lenBys := make([]byte, 4)
		n, err := body.Read(lenBys)
		if err != nil {
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
		return
	}
	if n != int(len) {
		return
	}

	return
}
