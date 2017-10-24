package udfs

import (
	. "asdf"
)

func protoRead(stream *TcpStream, request bool) (*ProtoHeader, IBinary, error) {
	bin, err := stream.Read()
	if nil != err {
		return nil, nil, err
	}

	hdr := &ProtoHeader{}
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

	if request {
		switch cmd {
		case cmdPush:
			msg = &ProtoTransfer{}
		case cmdDel, cmdTouch, cmdPull:
			msg = &ProtoIdentify{}
		}
	} else {
		switch cmd {
		case cmdPush, cmdDel, cmdTouch:
			msg = &ProtoError{}
		case cmdPull:
			if hdr.flag.Has(flagError) {
				msg = &ProtoError{}
			} else {
				msg = &ProtoTransfer{}
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

func replyOk(stream *TcpStream, cmd ProtoCmd) error {
	return replyError(stream, cmd, 0, Empty)
}

func replyError(stream *TcpStream, cmd ProtoCmd, Err int, Errs string) error {
	flag := flagResponse
	if Err != 0 {
		flag |= flagError
	}

	msg := &ProtoError{
		ProtoHeader: NewProtoHeader(cmd, flag),
		err:         int32(Err),
		errs:        []byte(Errs),
	}

	return protoWrite(stream, msg)
}

func replyFile(stream *TcpStream, cmd ProtoCmd, Time Time32, bkdr Bkdr, digest, content []byte) error {
	msg := &ProtoTransfer{
		ProtoHeader: NewProtoHeader(cmd, flagResponse),
		bkdr:        newbkdr(bkdr, digest),
		time:        Time,
		digest:      newdigest(digest, content),
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
	case *ProtoError:
		return obj.Error()
	case *ProtoTransfer:
		file := dbConf.File(obj.bkdr, obj.digest)
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
