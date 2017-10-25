package udfs

import (
	. "asdf"
	"encoding/hex"
	"fmt"
)

type DbEntry struct {
	time   Time32
	bkdr   Bkdr
	idir   byte
	digest [DigestSize]byte
}

func (me *DbEntry) String() string {
	return fmt.Sprintf("time:%v bkdr:%x dir:%d digest:%s",
		me.time.Unix(),
		me.bkdr,
		me.idir,
		hex.EncodeToString(me.digest[:]))
}

const sizeofDbEntry = SizeofByte + 2*SizeofInt32 + DigestSize

func (me *DbEntry) Size() int {
	return sizeofDbEntry
}

func (me *DbEntry) ToBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	Htonl(bin[0:], uint32(me.time))
	Htonl(bin[4:], uint32(me.bkdr))
	bin[8] = byte(me.idir)

	copy(bin[9:], me.digest[:])

	return nil
}

func (me *DbEntry) FromBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	me.time = Time32(Ntohl(bin[0:]))
	me.bkdr = Bkdr(Ntohl(bin[4:]))
	me.idir = bin[8]

	copy(me.digest[:], bin[9:])

	return nil
}
