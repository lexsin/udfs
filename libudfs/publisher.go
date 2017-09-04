package udfs

import (
	. "asdf"
)

// first call in main
func InitPublisher() {
	udfsInit(rolePublisher)

	go udfs.gc()
}

// api for publisher
func PublisherPush(bkdr Bkdr, digest, content []byte) error {
	var err error

	if dbExist(bkdr, digest) {
		err = udfs.leader(bkdr).touch(bkdr, digest)
	} else {
		err = udfs.leader(bkdr).push(bkdr, 0, digest, content)
		if nil != err {
			return err
		}

		// if leader error, push to leader's followers
		err = udfs.push(bkdr, 0, digest, content)
	}
	if nil != err {
		return err
	}

	_, err = dbAdd(bkdr, digest, 0)

	return err
}
