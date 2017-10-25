package udfs

import (
	. "asdf"
)

func tcpAddr(ip string) *TcpAddr {
	return NewTcpAddr(conf.Port, ip)
}

func newNode(ip string) *Node {
	return &Node{
		alive: true,
		addr:  tcpAddr(ip),
	}
}

type Node struct {
	alive bool
	addr  *TcpAddr
}

var loopback = tcpAddr("127.0.0.1")

func (me *Node) dial() (*TcpStream, error) {
	if roleConsumer == ep.role {
		return TcpStreamDial(loopback)
	} else {
		return TcpStreamDial(me.addr)
	}
}

func (me *Node) call(msg IBinary) error {
	stream, err := me.dial()
	if nil != err {
		Log.Info("dial error:%v", err)

		return err
	}
	defer stream.Close()

	err = protoWrite(stream, msg)
	if nil != err {
		return err
	}

	return recvResponse(stream)
}

func (me *Node) push(bkdr Bkdr, time Time32, digest, content []byte) error {
	msg := &ProtoTransfer{
		ProtoHeader: NewProtoHeader(cmdPush, 0),
		bkdr:        newbkdr(bkdr, digest),
		time:        newtime32(time),
		digest:      newdigest(digest, content),
		content:     content,
	}

	return me.call(msg)
}

func (me *Node) del(bkdr Bkdr, digest []byte) error {
	msg := &ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdDel, 0),
		bkdr:        newbkdr(bkdr, digest),
		digest:      digest,
	}

	return me.call(msg)
}

func (me *Node) pull(bkdr Bkdr, digest []byte) error {
	msg := &ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdPull, 0),
		bkdr:        newbkdr(bkdr, digest),
		digest:      digest,
	}

	// save content @recv
	return me.call(msg)
}

func (me *Node) touch(bkdr Bkdr, digest []byte) error {
	msg := &ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdTouch, 0),
		bkdr:        newbkdr(bkdr, digest),
		digest:      digest,
	}

	return me.call(msg)
}
