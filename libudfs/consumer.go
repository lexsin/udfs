package udfs

import (
	. "asdf"
	"net"
)

var loopback *net.TCPAddr

// first call in main
func InitConsumer() {
	udfsInit(roleConsumer)

	loopback = &net.TCPAddr{
		Port: conf.Port,
		IP:   net.ParseIP("127.0.0.1"),
	}
}

// api for consumer
func ConsumerPull(bkdr Bkdr, digest []byte) error {
	return udfs.leader(bkdr).pull(bkdr, digest)
}
