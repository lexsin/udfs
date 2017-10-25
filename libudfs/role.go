package udfs

import (
	. "asdf"
)

const (
	roleConsumer  Role = 0
	rolePublisher Role = 1
	roleBroker    Role = 2
	roleEnd       Role = 3
)

var udfsRoles = [roleEnd]string{
	roleConsumer:  "consumer",
	rolePublisher: "publisher",
	roleBroker:    "broker",
}

type Role int

func (me Role) IsGood() bool {
	return me >= 0 && me < roleEnd
}

func (me Role) String() string {
	if me.IsGood() {
		return udfsRoles[me]
	} else {
		return Unknow
	}
}
