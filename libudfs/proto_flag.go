package udfs

import (
	"encoding/binary"
)

type ProtoFlag uint16

func (me ProtoFlag) ToBinary(bin []byte) {
	binary.BigEndian.PutUint16(bin, uint16(me))
}

func (me *ProtoFlag) FromBinary(bin []byte) {
	*me = ProtoFlag(binary.BigEndian.Uint16(bin))
}

const (
	flagResponse ProtoFlag = 0x01 // only for response
	flagError    ProtoFlag = 0x02 // only for response
)

func (me ProtoFlag) Has(flag ProtoFlag) bool {
	return flag == (flag & me)
}

func (me ProtoFlag) IsGood() bool {
	return true
}

func (me ProtoFlag) String() string {
	buf := []byte("")

	Append := func(s string) {
		if len(buf) > 0 {
			buf = append(buf, '|')
		}

		buf = append(buf, []byte(s)...)
	}

	if me.Has(flagResponse) {
		Append("response")
	} else {
		Append("request")
	}

	if me.Has(flagError) {
		Append("error")
	}

	return string(buf)
}
