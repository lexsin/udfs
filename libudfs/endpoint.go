package udfs

import (
	"encoding/binary"
	"net"
	"os"
	"time"

	. "asdf"
)

type udfsEndPoint struct {
	nodes    []*udfsNode
	self     *udfsNode
	listener *net.TCPListener
	role     udfsRole
}

func newEndPoint(role udfsRole) *udfsEndPoint {
	count := len(conf.Nodes)

	ep := &udfsEndPoint{
		nodes: make([]*udfsNode, count+conf.Replication-1),
		role:  role,
	}

	for i := 0; i < count; i++ {
		ep.nodes[i] = newUdfsNode(conf.Nodes[i])
	}
	ep.self = ep.nodes[thisNodeID]

	for i := 0; i < conf.Replication-1; i++ {
		ep.nodes[count+i] = ep.nodes[i]
	}

	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: conf.Port,
		IP:   net.ParseIP("0.0.0.0"),
	})
	if nil != err {
		Log.Error("listen port:%d error:%v", conf.Port, err)

		os.Exit(StdErrListen)
	}
	ep.listener = listener

	return ep
}

func (me *udfsEndPoint) hash(bkdr Bkdr) int {
	return int(bkdr) % len(me.nodes)
}

func (me *udfsEndPoint) leader(bkdr Bkdr) *udfsNode {
	return me.nodes[me.hash(bkdr)]
}

func (me *udfsEndPoint) group(bkdr Bkdr) []*udfsNode {
	// count: 5
	// Replication: 2
	// nodes: 5+2-1=6

	// leader: 4
	// group: [4:5]
	// flower:[5:5]
	leader := me.hash(bkdr)

	return me.nodes[leader : leader+conf.Replication-1]
}

func (me *udfsEndPoint) followers(bkdr Bkdr) []*udfsNode {
	group := me.group(bkdr)

	return group[1:]
}

func (me *udfsEndPoint) Push(bkdr Bkdr, time Time32, digest, content []byte) error {
	file := dbconf.File(bkdr, digest)

	if !dbExist(bkdr, digest) {
		file.Save(content)
	}
	file.Touch(time)

	dbAdd(bkdr, digest, time)

	return me.push(bkdr, time, digest, content)
}

func (me *udfsEndPoint) push(bkdr Bkdr, time Time32, digest, content []byte) error {
	var err error

	if me.self == me.leader(bkdr) {
		followers := me.followers(bkdr)

		for _, node := range followers {
			err = node.push(bkdr, time, digest, content)
			if nil == err {
				return nil
			}
		}
	}

	return err
}

func (me *udfsEndPoint) Del(bkdr Bkdr, digest []byte) error {
	file := dbconf.File(bkdr, digest)

	if dbExist(bkdr, digest) {
		file.Delete()
	}

	dbDel(bkdr, digest)

	return me.del(bkdr, digest)
}

func (me *udfsEndPoint) del(bkdr Bkdr, digest []byte) error {
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

func (me *udfsEndPoint) Find(bkdr Bkdr, digest []byte) error {
	file := dbconf.File(bkdr, digest)

	if !dbExist(bkdr, digest) {
		return ErrNoExist
	} else if !file.Exist() {
		return ErrNoExist
	} else {
		return nil
	}
}

func (me *udfsEndPoint) find(bkdr Bkdr, digest []byte) error {
	var err error

	followers := me.followers(bkdr)

	for _, node := range followers {
		err = node.find(bkdr, digest)
		if nil == err {
			return nil
		}
	}

	return err
}

func (me *udfsEndPoint) Pull(bkdr Bkdr, digest []byte) error {
	file := dbconf.File(bkdr, digest)

	if dbExist(bkdr, digest) && file.Exist() {
		return nil
	}

	return me.pull(bkdr, digest)
}

func (me *udfsEndPoint) pull(bkdr Bkdr, digest []byte) error {
	var err error

	followers := me.followers(bkdr)

	for _, node := range followers {
		err = node.pull(bkdr, digest)
		if nil == err {
			return nil
		}
	}

	return err
}

func (me *udfsEndPoint) Touch(bkdr Bkdr, digest []byte) error {
	time := NowTime32()
	file := dbconf.File(bkdr, digest)

	file.Touch(time)
	dbAdd(bkdr, digest, time)

	return me.touch(bkdr, digest)
}

func (me *udfsEndPoint) touch(bkdr Bkdr, digest []byte) error {
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

func (me *udfsEndPoint) listen() {
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
func (me *udfsEndPoint) handle(stream *TcpStream) error {
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
		obj := msg.(*protoTransfer)

		err = me.Push(obj.bkdr, obj.time, obj.digest, obj.content)
	case cmdPull:
		obj := msg.(*protoIdentify)

		err = me.Pull(obj.bkdr, obj.digest)
	case cmdFind:
		obj := msg.(*protoIdentify)

		err = me.Find(obj.bkdr, obj.digest)
	case cmdDel:
		obj := msg.(*protoIdentify)

		err = me.Del(obj.bkdr, obj.digest)
	case cmdTouch:
		obj := msg.(*protoIdentify)

		err = me.Touch(obj.bkdr, obj.digest)
	}

	return err
}

func (me *udfsEndPoint) gc() {
	var bucket [2]byte
	var ticks uint64

	// Day = 3600*24*60 = 86400 Second
	// whole gc: 5*65536 = 327680 = 3.8 Day
	chTick := time.Tick(5 * time.Second)

	for {
		select {
		case <-chTick:
			binary.BigEndian.PutUint16(bucket[:], uint16(ticks))
			dbGc(bucket[:], func(file udfsFile) {
				file.Delete()
			})
			ticks++
		}
	}
}
