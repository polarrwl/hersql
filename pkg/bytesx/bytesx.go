package bytesx

import (
	"bytes"
	"encoding/binary"
)

func Bytes2Int32(bys []byte) int32 {
	buf := bytes.NewBuffer(bys)
	var data int32
	binary.Read(buf, binary.BigEndian, &data)
	return data
}
