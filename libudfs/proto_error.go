package udfs

import (
	"errors"
	"fmt"

	. "asdf"
)

// create/delete/find response
type ProtoError struct {
	ProtoHeader

	err int32
	// nerrs uint32 // errs length, just protocol, not delete this line

	errs []byte // maybe nil/empty
}

func (me *ProtoError) Error() error {
	if 0 == me.err {
		return nil
	} else if len(me.errs) > 0 {
		return errors.New(string(me.errs))
	} else {
		return NewError(int(me.err))
	}
}

func (me *ProtoError) String() string {
	errs := Empty
	if len(me.errs) > 0 {
		errs = string(me.errs)
	}

	return me.ProtoHeader.String() + fmt.Sprintf(" err:%d errs:%s", me.err, errs)
}

const ProtoErrorFixedSize = 2 * SizeofInt32

func (me *ProtoError) FixedSize() int {
	return ProtoErrorFixedSize
}

func (me *ProtoError) Size() int {
	return me.ProtoHeader.Size() + me.FixedSize() + len(me.errs)
}

func (me *ProtoError) ToBinary(bin []byte) error {
	hdr := &me.ProtoHeader
	err := hdr.ToBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// fixed ==> binary
	Htonl(bin[0:], uint32(me.err))
	Htonl(bin[4:], uint32(len(me.errs)))

	// dynamic ==> binary
	copy(bin[me.FixedSize():], me.errs)

	return nil
}

func (me *ProtoError) FromBinary(bin []byte) error {
	hdr := &me.ProtoHeader
	err := hdr.FromBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// binary ==> fixed
	me.err = int32(Ntohl(bin[0:]))
	nerrs := int(Ntohl(bin[4:]))

	offset := me.FixedSize()
	// binary ==> dynamic
	if nerrs > 0 {
		me.errs, offset = GetBytes(bin, offset, nerrs)
	}

	return nil
}
