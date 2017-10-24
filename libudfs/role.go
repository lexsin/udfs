package udfs

import (
	. "asdf"
)

const (
	roleConsumer  UdfsRole = 0
	rolePublisher UdfsRole = 1
	roleBroker    UdfsRole = 2
	roleEnd       UdfsRole = 3
)

var udfsRoles = [roleEnd]string{
	roleConsumer:  "consumer",
	rolePublisher: "publisher",
	roleBroker:    "broker",
}

type UdfsRole int

func (me UdfsRole) IsGood() bool {
	return me >= 0 && me < roleEnd
}

func (me UdfsRole) String() string {
	if me.IsGood() {
		return udfsRoles[me]
	} else {
		return Unknow
	}
}
