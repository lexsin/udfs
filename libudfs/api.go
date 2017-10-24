package udfs

import (
	. "asdf"
)

// broker api
// call in main
func BrokerMain() {
	initByRole(roleBroker)

	go ep.listen()

	ep.gc()
}

// consumer api
// first call in main
func InitConsumer() {
	initByRole(roleConsumer)
}

// consumer api
func ConsumerPull(bkdr Bkdr, digest []byte) error {
	err := ep.leader(bkdr).pull(bkdr, digest)
	if nil != err {
		return ep.pullFollowers(bkdr, digest)
	} else {
		return nil
	}
}

// publisher api
// first call in main
func InitPublisher() {
	initByRole(rolePublisher)

	go ep.gc()
}

// publisher api
func PublisherPush(bkdr Bkdr, digest, content []byte) error {
	var err error

	leader := ep.leader(bkdr)

	if dbExist(bkdr, digest) {
		err = leader.touch(bkdr, digest)
	} else {
		// 1. try push to leader
		// 2. if error, push to followers
		err = leader.push(bkdr, 0, digest, content)
		if nil != err {
			err = ep.pushFollowers(bkdr, 0, digest, content)
		}
	}
	if nil != err {
		return err
	}

	_, err = dbAdd(bkdr, digest, 0)

	return err
}
