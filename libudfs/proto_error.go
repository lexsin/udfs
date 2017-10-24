package udfs

import (
	"encoding/binary"
	"errors"
	"fmt"

	. "asdf"
)

// create/delete/find response
type ProtoError struct {
	ProtoHeader

	err int32
	// nerrs uint32 // errs length, just protocol, not delete this line

	errs []byte // maybe nil
}

func (me *ProtoError) Error() error {
	if 0 == me.err {
		return nil
	} else if nil != me.errs {
		return errors.New(string(me.errs))
	} else {
		return NewError(int(me.err))
	}
}

func (me *ProtoError) String() string {
	errs := Empty
	if nil != me.errs {
		errs = string(me.errs)
	}

	return me.ProtoHeader.String() + fmt.Sprintf(" err:%d errs:%s", me.err, errs)
}

func (me *ProtoError) FixedSize() int {
	return 2 * SizeofInt32
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
	binary.BigEndian.PutUint32(bin[0:], uint32(me.err))
	binary.BigEndian.PutUint32(bin[4:], uint32(len(me.errs)))

	// dynamic ==> binary
	Copy(bin[me.FixedSize():], me.errs)

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
	me.err = int32(binary.BigEndian.Uint32(bin[0:]))
	nerrs := int(binary.BigEndian.Uint32(bin[4:]))

	// binary ==> dynamic
	if nerrs > 0 {
		begin := me.FixedSize()
		me.errs = bin[begin : begin+nerrs]
	}

	return nil
}
