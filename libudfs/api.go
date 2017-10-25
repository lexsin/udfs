package udfs

import (
	. "asdf"
)

// broker api
// call in main
func BrokerMain() {
	initRole(roleBroker)

	go ep.listen()

	ep.gc()
}

// consumer api
// first call in main
func InitConsumer() {
	initRole(roleConsumer)
}

// consumer api
func ConsumerPull(bkdr Bkdr, digest []byte) (FileName, error) {
	err := ep.leader(bkdr).pull(bkdr, digest)
	if nil != err {
		// NOT pull from follers
		// do nothing, just error
		return Empty, err
	} else {
		filename := dbConf.File(bkdr, digest).name

		return filename, nil
	}
}

// publisher api
// first call in main
func InitPublisher() {
	initRole(rolePublisher)

	go ep.gc()
}

// publisher api
func PublisherPush(bkdr Bkdr, digest, content []byte) error {
	var err error

	leader := ep.leader(bkdr)

	if dbExist(bkdr, digest) {
		// 1. try push to leader
		// 2. if error, push to followers
		err = leader.touch(bkdr, digest)
		if nil != err {
			err = ep.touchFollowers(bkdr, digest)
		}
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
