package udfs

import (
	"fmt"

	. "asdf"
)

const protoVersion = 0

type ProtoHeader struct {
	version byte
	cmd     ProtoCmd
	flag    ProtoFlag
}

func NewProtoHeader(cmd ProtoCmd, flag ProtoFlag) ProtoHeader {
	return ProtoHeader{
		version: protoVersion,
		cmd:     cmd,
		flag:    flag,
	}
}

func (me *ProtoHeader) String() string {
	return fmt.Sprintf("version:%d cmd:%s flag:%s",
		me.version,
		me.cmd.String(),
		me.flag.String())
}

func (me *ProtoHeader) Size() int {
	return 2*SizeofByte + SizeofInt16 + SizeofInt32
}

func (me *ProtoHeader) ToBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	bin[0] = me.version
	bin[1] = byte(me.cmd)

	me.flag.ToBinary(bin[2:])

	return nil
}

func (me *ProtoHeader) FromBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	me.version = bin[0]
	me.cmd = ProtoCmd(bin[1])

	(&me.flag).FromBinary(bin[2:])

	return nil
}