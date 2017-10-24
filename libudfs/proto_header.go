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

const ProtoHeaderSize = 2*SizeofByte + SizeofInt16 + SizeofInt32

func (me *ProtoHeader) Size() int {
	return ProtoHeaderSize
}

func (me *ProtoHeader) ToBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	bin[0] = me.version
	bin[1] = byte(me.cmd)

	Htons(bin[2:], uint16(me.flag))

	return nil
}

func (me *ProtoHeader) FromBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	me.version = bin[0]
	me.cmd = ProtoCmd(bin[1])

	me.flag = ProtoFlag(Ntohs(bin[2:]))

	return nil
}
