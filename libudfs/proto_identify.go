package udfs

import (
	"encoding/binary"
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

func (me *ProtoIdentify) FixedSize() int {
	return 2 * SizeofInt32
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
	me.bkdr.ToBinary(bin[0:])
	binary.BigEndian.PutUint32(bin[4:], uint32(len(me.digest)))

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
	(&me.bkdr).FromBinary(bin[0:])
	ndigest := int(binary.BigEndian.Uint32(bin[4:]))

	if 0 == ndigest {
		return ErrEmpty
	}

	// binary ==> dyanmic
	begin := me.FixedSize()
	end := begin + ndigest
	me.digest = bin[begin:end]

	return nil
}
