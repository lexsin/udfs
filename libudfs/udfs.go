package udfs

import (
	. "asdf"
)

const (
	roleConsumer  udfsRole = 0
	rolePublisher udfsRole = 1
	roleBroker    udfsRole = 2
	roleEnd       udfsRole = 3
)

var udfsRoles = [roleEnd]string{
	roleConsumer:  "consumer",
	rolePublisher: "publisher",
	roleBroker:    "broker",
}

type udfsRole int

func (me udfsRole) IsGood() bool {
	return me >= 0 && me < roleEnd
}

func (me udfsRole) String() string {
	if me.IsGood() {
		return udfsRoles[me]
	} else {
		return Unknow
	}
}

var udfs *udfsEndPoint

func udfsInit(role udfsRole) {
	preInit()

	switch role {
	case rolePublisher:
		dbInit()
	case roleBroker:
		fileInit()
		dbInit()
	case roleConsumer:
		// do nothing
	}

	udfs = newEndPoint(role)
}
