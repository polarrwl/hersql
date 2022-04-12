package bytesx

import (
	"bytes"
	"encoding/binary"
)

func Bytes2Uint32(bys []byte) uint32 {
	buf := bytes.NewBuffer(bys)
	var data uint32
	binary.Read(buf, binary.BigEndian, &data)
	return data
}
