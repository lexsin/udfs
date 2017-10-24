package udfs

import (
	. "asdf"
)

var ep *EndPoint

func init() {
	initEnv()
	initConf()
}

func initByRole(role UdfsRole) {
	switch role {
	case rolePublisher:
		dbInit()
	case roleBroker:
		fileInit()

		dbInit()
	case roleConsumer:
		DoNothing()
	}

	ep = newEndPoint(role)
}
