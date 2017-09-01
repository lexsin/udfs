package udfs

import (
	"net"

	. "asdf"
)

type udfsNode struct {
	alive bool
	addr  *net.TCPAddr
}

func newUdfsNode(ip string) *udfsNode {
	return &udfsNode{
		alive: true,
		addr: &net.TCPAddr{
			Port: conf.Port,
			IP:   net.ParseIP(ip),
		},
	}
}

func (me *udfsNode) dial() (*TcpStream, error) {
	if roleConsumer == udfs.role {
		return TcpStreamDial(loopback)
	} else {
		return TcpStreamDial(me.addr)
	}
}

func (me *udfsNode) call(msg IBinary) error {
	stream, err := me.dial()
	if nil != err {
		return err
	}
	defer stream.Close()

	err = protoWrite(stream, msg)
	if nil != err {
		return err
	}

	return recvResponse(stream)
}

func (me *udfsNode) push(bkdr Bkdr, time Time32, digest, content []byte) error {
	if nil == digest {
		digest = DeftDigester.Digest(content)
	}

	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	if 0 == time {
		time = NowTime32()
	}

	return me.call(&protoTransfer{
		protoHeader: NewProtoHeader(cmdPush, 0),
		bkdr:        bkdr,
		time:        time,
		digest:      digest,
		content:     content,
	})
}

func (me *udfsNode) del(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&protoIdentify{
		protoHeader: NewProtoHeader(cmdDel, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}

func (me *udfsNode) find(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&protoIdentify{
		protoHeader: NewProtoHeader(cmdFind, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}

func (me *udfsNode) pull(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&protoIdentify{
		protoHeader: NewProtoHeader(cmdPull, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}

func (me *udfsNode) touch(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&protoIdentify{
		protoHeader: NewProtoHeader(cmdTouch, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}
