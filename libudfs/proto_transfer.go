package udfs

import (
	"encoding/hex"
	"fmt"

	. "asdf"
)

// create request
// get response
type ProtoTransfer struct {
	ProtoHeader

	bkdr Bkdr
	time Time32 // create time, like C: time_t
	// ndigest  uint32 // just protocol, not delete this line
	// ncontent uint32 // just protocol, not delete this line

	digest  []byte
	content []byte
}

func (me *ProtoTransfer) String() string {
	return me.ProtoHeader.String() + fmt.Sprintf(" bkdr:%x digest:%s content:%s",
		me.bkdr,
		hex.EncodeToString(me.digest),
		hex.EncodeToString(me.content))
}

const ProtoTransferFixedSize = 4 * SizeofInt32

func (me *ProtoTransfer) FixedSize() int {
	return ProtoTransferFixedSize
}

func (me *ProtoTransfer) Size() int {
	return me.ProtoHeader.Size() + me.FixedSize() + len(me.digest) + len(me.content)
}

func (me *ProtoTransfer) ToBinary(bin []byte) error {
	hdr := &me.ProtoHeader
	err := hdr.ToBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// fixed ==> binary
	Htonl(bin[0:], uint32(me.bkdr))
	Htonl(bin[4:], uint32(me.time))
	Htonl(bin[8:], uint32(len(me.digest)))
	Htonl(bin[12:], uint32(len(me.content)))

	// dynamic ==> binary
	begin := me.FixedSize()
	copy(bin[begin:], me.digest)

	begin += len(me.digest)
	copy(bin[begin:], me.content)

	return nil
}

func (me *ProtoTransfer) FromBinary(bin []byte) error {
	hdr := &me.ProtoHeader
	err := hdr.FromBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// binary ==> fixed
	me.bkdr = Bkdr(Ntohl(bin[0:]))
	me.time = Time32(Ntohl(bin[4:]))
	ndigest := int(Ntohl(bin[8:]))
	ncontent := int(Ntohl(bin[12:]))

	if 0 == ndigest {
		return ErrEmpty
	} else if 0 == ncontent {
		return ErrEmpty
	}
	offset := me.FixedSize()

	// binary ==> dynamic
	me.digest, offset = GetBytes(bin, offset, ndigest)
	me.content, offset = GetBytes(bin, offset, ncontent)

	return nil
}
