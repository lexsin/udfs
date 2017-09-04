package udfs

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	. "asdf"
)

type protoCmd byte

const (
	// publisher ==> leader
	// leader ==> follower
	cmdPush protoCmd = 0
	// consumer ==> leader
	// leader ==> follower
	cmdPull protoCmd = 1
	// none
	cmdDel protoCmd = 2
	// leader ==> follower
	cmdFind protoCmd = 3
	// publisher ==> leader
	// leader ==> follower
	cmdTouch protoCmd = 4
	cmdEnd   protoCmd = 5
)

var cmdStrings = [cmdEnd]string{
	cmdPush:  "push",
	cmdPull:  "pull",
	cmdDel:   "del",
	cmdFind:  "find",
	cmdTouch: "touch",
}

func (me protoCmd) IsGood() bool {
	return me >= 0 && me < cmdEnd
}

func (me protoCmd) String() string {
	return cmdStrings[me]
}

type protoFlag uint16

const (
	flagResponse protoFlag = 0x01 // only for response
	flagError    protoFlag = 0x02 // only for response
)

func (me protoFlag) Has(flag protoFlag) bool {
	return flag == (flag & me)
}

func (me protoFlag) IsGood() bool {
	return true
}

func (me protoFlag) String() string {
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

const protoVersion = 0

type protoHeader struct {
	version byte
	cmd     protoCmd
	flag    protoFlag
}

func NewProtoHeader(cmd protoCmd, flag protoFlag) protoHeader {
	return protoHeader{
		version: protoVersion,
		cmd:     cmd,
		flag:    flag,
	}
}

func (me *protoHeader) String() string {
	return fmt.Sprintf("version:%d cmd:%s flag:%s",
		me.version,
		me.cmd.String(),
		me.flag.String())
}

func (me *protoHeader) Size() int {
	return 2*SizeofByte + SizeofInt16 + SizeofInt32
}

func (me *protoHeader) ToBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	bin[0] = me.version
	bin[1] = byte(me.cmd)

	binary.BigEndian.PutUint16(bin[2:], uint16(me.flag))

	return nil
}

func (me *protoHeader) FromBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	me.version = bin[0]
	me.cmd = protoCmd(bin[1])

	me.flag = protoFlag(binary.BigEndian.Uint16(bin[2:]))

	return nil
}

// create/delete/find response
type protoError struct {
	protoHeader

	err int32
	// nerrs uint32 // errs length, just protocol

	errs []byte // maybe nil
}

func (me *protoError) Error() error {
	if 0 == me.err {
		return nil
	} else if nil != me.errs {
		return errors.New(string(me.errs))
	} else {
		return NewError(int(me.err))
	}
}

func (me *protoError) String() string {
	errs := Empty
	if nil != me.errs {
		errs = string(me.errs)
	}

	return me.protoHeader.String() + fmt.Sprintf(" err:%d errs:%s", me.err, errs)
}

func (me *protoError) FixedSize() int {
	return 2 * SizeofInt32
}

func (me *protoError) Size() int {
	return me.protoHeader.Size() + me.FixedSize() + Len(me.errs)
}

func (me *protoError) ToBinary(bin []byte) error {
	hdr := &me.protoHeader
	err := hdr.ToBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// fixed ==> binary
	binary.BigEndian.PutUint32(bin[0:], uint32(me.err))
	binary.BigEndian.PutUint32(bin[4:], uint32(Len(me.errs)))

	// dynamic ==> binary
	Copy(bin[me.FixedSize():], me.errs)

	return nil
}

func (me *protoError) FromBinary(bin []byte) error {
	hdr := &me.protoHeader
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

// delete/find/get repuest
type protoIdentify struct {
	protoHeader

	bkdr Bkdr
	// ndigest uint32 // just protocol

	digest []byte
}

func (me *protoIdentify) String() string {
	return me.protoHeader.String() + fmt.Sprintf(" bkdr:%x digest:%s",
		me.bkdr,
		hex.EncodeToString(me.digest))
}

func (me *protoIdentify) FixedSize() int {
	return 2 * SizeofInt32
}

func (me *protoIdentify) Size() int {
	return me.protoHeader.Size() + me.FixedSize() + len(me.digest)
}

func (me *protoIdentify) ToBinary(bin []byte) error {
	hdr := &me.protoHeader
	err := hdr.ToBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// fixed ==> binary
	binary.BigEndian.PutUint32(bin[0:], uint32(me.bkdr))
	binary.BigEndian.PutUint32(bin[4:], uint32(Len(me.digest)))

	// dynamic ==> binary
	copy(bin[me.FixedSize():], me.digest)

	return nil
}

func (me *protoIdentify) FromBinary(bin []byte) error {
	hdr := &me.protoHeader
	err := hdr.FromBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// binary ==> fixed
	me.bkdr = Bkdr(binary.BigEndian.Uint32(bin[0:]))
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

// create request
// get response
type protoTransfer struct {
	protoHeader

	bkdr Bkdr
	time Time32 // create time, like C: time_t
	// ndigest  uint32 // just protocol
	// ncontent uint32 // just protocol

	digest  []byte
	content []byte
}

func (me *protoTransfer) String() string {
	return me.protoHeader.String() + fmt.Sprintf(" bkdr:%x digest:%s content:%s",
		me.bkdr,
		hex.EncodeToString(me.digest),
		hex.EncodeToString(me.content))
}

func (me *protoTransfer) FixedSize() int {
	return 4 * SizeofInt32
}

func (me *protoTransfer) Size() int {
	return me.protoHeader.Size() + me.FixedSize() + len(me.digest) + len(me.content)
}

func (me *protoTransfer) ToBinary(bin []byte) error {
	hdr := &me.protoHeader
	err := hdr.ToBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// fixed ==> binary
	binary.BigEndian.PutUint32(bin[0:], uint32(me.bkdr))
	binary.BigEndian.PutUint32(bin[4:], uint32(me.time))
	binary.BigEndian.PutUint32(bin[8:], uint32(Len(me.digest)))
	binary.BigEndian.PutUint32(bin[12:], uint32(Len(me.content)))

	// dynamic ==> binary
	begin := me.FixedSize()
	copy(bin[begin:], me.digest)

	begin += Len(me.digest)
	copy(bin[begin:], me.content)

	return nil
}

func (me *protoTransfer) FromBinary(bin []byte) error {
	hdr := &me.protoHeader
	err := hdr.FromBinary(bin[0:])
	if nil != err {
		return err
	}
	bin = bin[hdr.Size():]

	// binary ==> fixed
	me.bkdr = Bkdr(binary.BigEndian.Uint32(bin[0:]))
	me.time = Time32(binary.BigEndian.Uint32(bin[4:]))
	ndigest := int(binary.BigEndian.Uint32(bin[8:]))
	ncontent := int(binary.BigEndian.Uint32(bin[12:]))

	if 0 == ndigest {
		return ErrEmpty
	} else if 0 == ncontent {
		return ErrEmpty
	}

	// binary ==> dynamic
	begin := me.FixedSize()
	end := begin + ndigest
	me.digest = bin[begin:end]

	begin = end
	end += ncontent
	me.content = bin[begin:end]

	return nil
}

func protoRead(stream *TcpStream, request bool) (*protoHeader, IBinary, error) {
	bin, err := stream.Read()
	if nil != err {
		return nil, nil, err
	}

	hdr := &protoHeader{}
	err = hdr.FromBinary(bin)
	if nil != err {
		Log.Info("read proto header error:%v", err)

		return nil, nil, err
	}

	isRequest := !hdr.flag.Has(flagResponse)
	if request != isRequest {
		Log.Info("read proto header dir error")

		return nil, nil, ErrBadProto
	}

	cmd := hdr.cmd
	if !cmd.IsGood() {
		Log.Info("invalid proto cmd:%d", cmd)

		return nil, nil, ErrBadProto
	}

	var msg IBinary

	switch cmd {
	case cmdPush:
		if request {
			msg = &protoTransfer{}
		} else {
			msg = &protoError{}
		}
	case cmdFind, cmdDel, cmdTouch:
		if request {
			msg = &protoIdentify{}
		} else {
			msg = &protoError{}
		}
	case cmdPull:
		if request {
			msg = &protoIdentify{}
		} else {
			if hdr.flag.Has(flagError) {
				msg = &protoError{}
			} else {
				msg = &protoTransfer{}
			}
		}
	}

	if err = msg.FromBinary(bin); nil != err {
		Log.Info("read proto error:%v", err)

		return nil, nil, err
	} else {
		return hdr, msg, nil
	}
}

func protoWrite(stream *TcpStream, msg IBinary) error {
	bin := make([]byte, msg.Size())
	err := msg.ToBinary(bin)
	if nil != err {
		Log.Info("write proto error:%v", err)

		return err
	}

	return stream.Write(bin)
}

func replyOk(stream *TcpStream, cmd protoCmd) error {
	return replyError(stream, cmd, 0, Empty)
}

func replyError(stream *TcpStream, cmd protoCmd, Err int, Errs string) error {
	var errs []byte

	if Empty != Errs {
		errs = []byte(Errs)
	}

	flag := flagResponse
	if Err != 0 {
		flag |= flagError
	}

	msg := &protoError{
		protoHeader: NewProtoHeader(cmd, flag),
		err:         int32(Err),
		errs:        errs,
	}

	return protoWrite(stream, msg)
}

func replyFile(stream *TcpStream, cmd protoCmd, Time Time32, bkdr Bkdr, digest, content []byte) error {
	if nil == digest {
		digest = DeftDigester.Digest(content)
	}

	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	msg := &protoTransfer{
		protoHeader: NewProtoHeader(cmd, flagResponse),
		bkdr:        bkdr,
		time:        Time,
		digest:      digest,
		content:     content,
	}

	return protoWrite(stream, msg)
}

func recvResponse(stream *TcpStream) error {
	_, msg, err := protoRead(stream, false)
	if nil != err {
		return err
	}

	switch obj := msg.(type) {
	case *protoError:
		return obj.Error()
	case *protoTransfer:
		file := dbconf.File(obj.bkdr, obj.digest)
		if err := file.Save(obj.content); nil != err {
			return err
		}

		if err := file.Touch(obj.time); nil != err {
			return err
		}

		_, err := dbAdd(obj.bkdr, obj.digest, obj.time)

		return err
	default:
		return ErrBadIntf
	}
}
