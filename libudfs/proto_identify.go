package udfs

import (
	"encoding/hex"
	"fmt"

	. "asdf"
)

// delete/find/get repuest
type ProtoIdentify struct {
	ProtoHeader

	bkdr Bkdr
	// ndigest uint32 // just protocol, not delete this line

	digest []byte
}

func (me *ProtoIdentify) String() string {
	return me.ProtoHeader.String() + fmt.Sprintf(" bkdr:%x digest:%s",
		me.bkdr,
		hex.EncodeToString(me.digest))
}

const sizeofProtoIdentifyFixed = 2 * SizeofInt32

func (me *ProtoIdentify) FixedSize() int {
	return sizeofProtoIdentifyFixed
}

func (me *ProtoIdentify) Size() int {
	return me.ProtoHeader.Size() + me.FixedSize() + len(me.digest)
}

func (me *ProtoIdentify) ToBinary(bin []byte) error {
	hdr := &me.ProtoHeader
	err := hdr.ToBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// fixed ==> binary
	Htonl(bin[0:], uint32(me.bkdr))
	Htonl(bin[4:], uint32(len(me.digest)))

	// dynamic ==> binary
	copy(bin[me.FixedSize():], me.digest)

	return nil
}

func (me *ProtoIdentify) FromBinary(bin []byte) error {
	hdr := &me.ProtoHeader
	err := hdr.FromBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// binary ==> fixed
	me.bkdr = Bkdr(Ntohl(bin[0:]))
	ndigest := int(Ntohl(bin[4:]))
	if 0 == ndigest {
		return ErrEmpty
	}
	offset := me.FixedSize()

	// binary ==> dyanmic
	me.digest, offset = GetBytes(bin, offset, ndigest)

	return nil
}
