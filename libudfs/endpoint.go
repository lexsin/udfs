package udfs

import (
	"encoding/binary"
	"os"
	"time"

	. "asdf"
)

var ep *EndPoint

func initEndPoint(role Role) {
	ep = newEndPoint(role)
}

func newEndPoint(role Role) *EndPoint {
	count := len(conf.Nodes)
	more := conf.Replication - 1

	ep := &EndPoint{
		nodes: make([]*Node, count+more),
		role:  role,
	}

	for i := 0; i < count; i++ {
		ep.nodes[i] = newNode(conf.Nodes[i])
	}

	for i := 0; i < more; i++ {
		ep.nodes[count+i] = ep.nodes[i]
	}

	if role != roleConsumer {
		listener, err := ListenTcp(conf.Port, "0.0.0.0")
		if nil != err {
			Log.Error("listen port:%d error:%v", conf.Port, err)

			os.Exit(StdErrListen)
		}
		ep.listener = listener
	}

	return ep
}

type EndPoint struct {
	nodes    []*Node
	listener *TcpListener
	role     Role
}

func (me *EndPoint) inode(bkdr Bkdr) int {
	return int(bkdr) % len(me.nodes)
}

func (me *EndPoint) self() *Node {
	return me.nodes[thisNodeID]
}

func (me *EndPoint) leader(bkdr Bkdr) *Node {
	return me.nodes[me.inode(bkdr)]
}

func (me *EndPoint) group(bkdr Bkdr) []*Node {
	// count: 5
	// Replication: 2
	// nodes: 5+2-1=6

	// leader: 4
	// group: [4:5]
	// flower:[5:5]
	iLeader := me.inode(bkdr)

	return me.nodes[iLeader : iLeader+conf.Replication-1]
}

func (me *EndPoint) followers(bkdr Bkdr) []*Node {
	group := me.group(bkdr)

	return group[1:]
}

func (me *EndPoint) push(bkdr Bkdr, time Time32, digest, content []byte) error {
	file := dbConf.File(bkdr, digest)

	if !dbExist(bkdr, digest) {
		file.Save(content)
	}
	file.Touch(time)

	dbAdd(bkdr, digest, time)

	if me.self() == me.leader(bkdr) {
		// leader should re-do it to follers
		return me.pushFollowers(bkdr, time, digest, content)
	} else {
		return nil
	}
}

func (me *EndPoint) pushFollowers(bkdr Bkdr, time Time32, digest, content []byte) error {
	var err error

	followers := me.followers(bkdr)

	for _, node := range followers {
		err := node.push(bkdr, time, digest, content)
		if nil == err {
			return nil
		}
	}

	return err
}

func (me *EndPoint) del(bkdr Bkdr, digest []byte) error {
	file := dbConf.File(bkdr, digest)

	if dbExist(bkdr, digest) {
		file.Delete()
	}

	dbDel(bkdr, digest)

	if me.self() == me.leader(bkdr) {
		// leader should re-do it to follers
		return me.delFollowers(bkdr, digest)
	} else {
		return nil
	}
}

func (me *EndPoint) delFollowers(bkdr Bkdr, digest []byte) error {
	var err error

	followers := me.followers(bkdr)

	for _, node := range followers {
		err = node.del(bkdr, digest)
		if nil == err {
			return nil
		}
	}

	return err
}

func (me *EndPoint) pull(bkdr Bkdr, digest []byte) error {
	file := dbConf.File(bkdr, digest)

	if dbExist(bkdr, digest) && file.Exist() {
		// file exist @local
		return nil
	} else {
		return me.pullFollowers(bkdr, digest)
	}
}

func (me *EndPoint) pullFollowers(bkdr Bkdr, digest []byte) error {
	var err error

	followers := me.followers(bkdr)

	for _, node := range followers {
		// pull file from node, and save local
		err = node.pull(bkdr, digest)
		if nil == err {
			return nil
		}
	}

	return err
}

func (me *EndPoint) touch(bkdr Bkdr, digest []byte) error {
	time := NowTime32()
	file := dbConf.File(bkdr, digest)

	file.Touch(time)
	dbAdd(bkdr, digest, time)

	if me.self() == me.leader(bkdr) {
		// leader should re-do it to follers
		return me.touchFollowers(bkdr, digest)
	} else {
		return nil
	}
}

func (me *EndPoint) touchFollowers(bkdr Bkdr, digest []byte) error {
	var err error

	followers := me.followers(bkdr)

	for _, node := range followers {
		err = node.touch(bkdr, digest)
		if nil == err {
			return nil
		}
	}

	return err
}

func (me *EndPoint) listen() {
	for {
		conn, err := me.listener.AcceptTCP()
		if nil != err {
			Log.Error("accept error:%v", err)

			continue
		}

		go me.handle(NewTcpStream(conn))
	}
}

// request handler
//
func (me *EndPoint) handle(stream *TcpStream) error {
	var stderr = 1

	hdr, msg, err := protoRead(stream, true)
	if nil != err {
		return err
	}

	defer func() {
		if nil != err {
			replyError(stream, hdr.cmd, stderr, err.Error())
		} else {
			replyOk(stream, hdr.cmd)
		}
	}()

	switch hdr.cmd {
	case cmdPush:
		obj := msg.(*ProtoTransfer)

		err = me.push(obj.bkdr, obj.time, obj.digest, obj.content)
	case cmdPull:
		obj := msg.(*ProtoIdentify)

		err = me.pull(obj.bkdr, obj.digest)
	case cmdDel:
		obj := msg.(*ProtoIdentify)

		err = me.del(obj.bkdr, obj.digest)
	case cmdTouch:
		obj := msg.(*ProtoIdentify)

		err = me.touch(obj.bkdr, obj.digest)
	}

	return err
}

func (me *EndPoint) gc() {
	var bucket [2]byte
	var ticks uint64

	// Day = 3600*24*60 = 86400 Second
	// whole gc: 5*65536 = 327680 = 3.8 Day
	chTick := time.Tick(5 * time.Second)

	for {
		select {
		case <-chTick:
			binary.BigEndian.PutUint16(bucket[:], uint16(ticks))
			ticks++

			dbGc(bucket[:], func(file UdfsFile) {
				file.Delete()
			})
		}
	}
}
