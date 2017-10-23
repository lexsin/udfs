package udfs

import (
	"net"

	. "asdf"
)

type UdfsNode struct {
	alive bool
	addr  *net.TCPAddr
}

func newUdfsNode(ip string) *UdfsNode {
	return &UdfsNode{
		alive: true,
		addr: &net.TCPAddr{
			Port: conf.Port,
			IP:   net.ParseIP(ip),
		},
	}
}

func (me *UdfsNode) dial() (*TcpStream, error) {
	if roleConsumer == udfs.role {
		return TcpStreamDial(loopback)
	} else {
		return TcpStreamDial(me.addr)
	}
}

func (me *UdfsNode) call(msg IBinary) error {
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

func (me *UdfsNode) push(bkdr Bkdr, time Time32, digest, content []byte) error {
	if nil == digest {
		digest = DeftDigester.Digest(content)
	}

	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	if 0 == time {
		time = NowTime32()
	}

	return me.call(&ProtoTransfer{
		ProtoHeader: NewProtoHeader(cmdPush, 0),
		bkdr:        bkdr,
		time:        time,
		digest:      digest,
		content:     content,
	})
}

func (me *UdfsNode) del(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdDel, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}

func (me *UdfsNode) find(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdFind, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}

func (me *UdfsNode) pull(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdPull, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}

func (me *UdfsNode) touch(bkdr Bkdr, digest []byte) error {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return me.call(&ProtoIdentify{
		ProtoHeader: NewProtoHeader(cmdTouch, 0),
		bkdr:        bkdr,
		digest:      digest,
	})
}
