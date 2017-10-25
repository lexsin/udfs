package udfs

import (
	. "asdf"
	"encoding/hex"
	"os"

	"github.com/boltdb/bolt"
)

var db *bolt.DB

func newbkdr(bkdr Bkdr, digest []byte) Bkdr {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	return bkdr
}

func newdigest(digest, content []byte) []byte {
	if nil == digest {
		digest = DeftDigester.Digest(content)
	}

	return digest
}

func newtime32(time Time32) Time32 {
	if 0 == time {
		time = NowTime32()
	}

	return time
}

func dbBucket(bkdr Bkdr) []byte {
	bucket := [2]byte{}

	Htons(bucket[:], uint16(bkdr))

	return bucket[:]
}

func dbGc(bucket []byte, fgc func(file UdfsFile)) {
	now := NowTime32()

	db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if nil == b {
			return nil
		}

		b.ForEach(func(k, v []byte) error {
			e := &DbEntry{}
			e.FromBinary(v)

			if e.time+conf.Live < now {
				b.Delete(k)

				go fgc(dbConf.File(e.bkdr, e.digest[:]))
			}

			return nil
		})

		return nil
	})
}

func dbExist(bkdr Bkdr, digest []byte) bool {
	entry, _ := dbGet(bkdr, digest)

	return nil != entry
}

func dbGet(bkdr Bkdr, digest []byte) (*DbEntry, error) {
	entry := &DbEntry{}

	bkdr = newbkdr(bkdr, digest)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket(bkdr))
		if nil == b {
			return ErrNoExist
		}

		v := b.Get(digest)
		if nil == v {
			return ErrNoExist
		}

		return entry.FromBinary(v)
	})
	if nil != err {
		Log.Error("db get bkdr:%x digest:%s error:%v", bkdr, hex.EncodeToString(digest), err.Error())

		return nil, err
	} else {
		return entry, nil
	}
}

func dbAdd(bkdr Bkdr, digest []byte, mtime Time32) (*DbEntry, error) {
	bkdr = newbkdr(bkdr, digest)
	mtime = newtime32(mtime)

	entry := &DbEntry{
		time: mtime,
		bkdr: bkdr,
		idir: dbConf.idir(bkdr),
	}
	copy(entry.digest[:], digest)

	bin, err := ToBinary(entry)
	if nil != err {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dbBucket(bkdr))
		if nil != err {
			return err
		}

		return b.Put(digest, bin)
	})
	if nil != err {
		Log.Error("db add %s error:%v", entry.String(), err.Error())

		return nil, err
	} else {
		return entry, nil
	}
}

func dbDel(bkdr Bkdr, digest []byte) error {
	bkdr = newbkdr(bkdr, digest)

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucket(bkdr))
		if nil != b {
			return b.Delete(digest)
		} else {
			return nil
		}
	})
	if nil != err {
		Log.Error("db del bkdr:%x digest:%s error:%v", bkdr, hex.EncodeToString(digest), err.Error())
	}

	return err
}

func dbDiskLoadBalance() error {
	entry := &DbEntry{}

	entryHandle := func(k, v []byte) error {
		if err := entry.FromBinary(v); nil == err {
			oldPath := dbConfOld.path(entry.bkdr)
			newPath := dbConf.path(entry.bkdr)
			os.MkdirAll(newPath.String(), 0775)

			oldFile := dbConfOld.file(oldPath, entry.digest[:])
			newFile := dbConf.file(newPath, entry.digest[:])

			os.Rename(oldFile.String(), newFile.String())
		}

		return nil
	}

	bucketHandle := func(name []byte, b *bolt.Bucket) error {
		return b.ForEach(entryHandle)
	}

	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(bucketHandle)
	})
}

// just for publisher/broker
func initDb(role Role) {
	if role != roleConsumer {
		bdb, err := bolt.Open(conf.DbFileName.Abs().String(), 0755, nil)
		if nil != err {
			os.Exit(StdErrBadFile)
		}

		db = bdb
	}
}
