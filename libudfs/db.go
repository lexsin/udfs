package udfs

import (
	. "asdf"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

const (
	dbFile     FileName = "udfs.db"
	dbJsonConf FileName = "udfs.json"
)

var db *bolt.DB
var dbconf = &dbConf{}
var dbconfold = &dbConf{}

type dbConf struct {
	Dirs []string `json:"dirs"`
}

func (me *dbConf) idir(bkdr Bkdr) byte {
	return byte(bkdr % Bkdr(len(me.Dirs)))
}

func (me *dbConf) path(bkdr Bkdr) udfsFile {
	var b [4]byte
	var s [8]byte

	binary.BigEndian.PutUint32(b[:], uint32(bkdr))
	hex.Encode(s[:], b[:])

	idir := me.idir(bkdr)
	path := filepath.Join(dbconf.Dirs[idir], string(s[0:4]), string(s[4:8]))

	return udfsFile{
		name: FileName(path),
		idir: idir,
	}
}

func (me *dbConf) file(path udfsFile, digest []byte) udfsFile {
	file := filepath.Join(path.String(), hex.EncodeToString(digest))

	return udfsFile{
		name: FileName(file),
		idir: path.idir,
	}
}

func (me *dbConf) File(bkdr Bkdr, digest []byte) udfsFile {
	return me.file(me.path(bkdr), digest)
}

func (me *dbConf) eq() bool {
	if len(me.Dirs) != len(conf.Dirs) {
		return false
	}

	for i, dir := range me.Dirs {
		if dir != conf.Dirs[i] {
			return false
		}
	}

	return true
}

func (me *dbConf) check() error {
	for _, path := range me.Dirs {
		if !FileName(path).DirExist() {
			return ErrNoDir
		}
	}

	return nil
}

const sizeofDbEntry = SizeofByte + 2*SizeofInt32 + DigestSize

type dbEntry struct {
	time   Time32
	bkdr   Bkdr
	idir   byte
	digest [DigestSize]byte
}

func dbBucketKey(bkdr Bkdr) []byte {
	var bucket [2]byte

	binary.BigEndian.PutUint16(bucket[:], uint16(bkdr))

	return bucket[:]
}

func (me *dbEntry) String() string {
	return fmt.Sprintf("time:%v bkdr:%x dir:%d digest:%s",
		me.time.Unix(),
		me.bkdr,
		me.idir,
		hex.EncodeToString(me.digest[:]))
}

func (me *dbEntry) Size() int {
	return SizeofByte + 2*SizeofInt32 + DigestSize
}

func (me *dbEntry) ToBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	binary.BigEndian.PutUint32(bin[0:], uint32(me.time))
	binary.BigEndian.PutUint32(bin[4:], uint32(me.bkdr))
	bin[8] = byte(me.idir)

	copy(bin[9:], me.digest[:])

	return nil
}

func (me *dbEntry) FromBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	me.time = Time32(binary.BigEndian.Uint32(bin[0:]))
	me.bkdr = Bkdr(binary.BigEndian.Uint32(bin[4:]))
	me.idir = bin[8]

	copy(me.digest[:], bin[9:])

	return nil
}

func dbGc(bucket []byte, fgc func(file udfsFile)) {
	now := NowTime32()

	db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if nil == b {
			return nil
		}

		b.ForEach(func(k, v []byte) error {
			e := &dbEntry{}
			e.FromBinary(v)

			if e.time+conf.Live < now {
				b.Delete(k)

				go fgc(dbconf.File(e.bkdr, e.digest[:]))
			}
			// todo
			// db gc
			// file gc
			return nil
		})

		return nil
	})
}

func dbExist(bkdr Bkdr, digest []byte) bool {
	entry, _ := dbGet(bkdr, digest)

	return nil != entry
}

func dbGet(bkdr Bkdr, digest []byte) (*dbEntry, error) {
	entry := &dbEntry{}

	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucketKey(bkdr))
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

func dbAdd(bkdr Bkdr, digest []byte, mtime Time32) (*dbEntry, error) {
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	if 0 == mtime {
		mtime = NowTime32()
	}

	entry := &dbEntry{
		time: mtime,
		bkdr: bkdr,
		idir: dbconf.idir(bkdr),
	}
	copy(entry.digest[:], digest)

	bin, err := ToBinary(entry)
	if nil != err {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dbBucketKey(bkdr))
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
	if 0 == bkdr {
		bkdr = DeftBkdrer.Bkdr(digest)
	}

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(dbBucketKey(bkdr))
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
	entry := &dbEntry{}

	entryHandle := func(k, v []byte) error {
		if err := entry.FromBinary(v); nil == err {
			oldPath := dbconfold.path(entry.bkdr)
			newPath := dbconf.path(entry.bkdr)
			os.MkdirAll(newPath.String(), 0775)

			oldFile := dbconfold.file(oldPath, entry.digest[:])
			newFile := dbconf.file(newPath, entry.digest[:])

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

func dbLoadConf() error {
	// just for broker
	if roleBroker != udfs.role {
		return nil
	}

	filename := dbJsonConf.Abs()
	if filename.Exist() {
		// db config exist, load it
		err := filename.LoadJson(dbconf)
		if nil != err {
			return err
		}

		err = dbconf.check()
		if nil != err {
			return err
		}

		if !dbconf.eq() {
			// self is broker
			// db config != etcd config
			// disk load balance
			if err = dbDiskLoadBalance(); nil != err {
				return err
			}
		}
	} else {
		// db config NOT exist, load it from etcd
		dbconf.Dirs = conf.Dirs

		err := dbconf.check()
		if nil != err {
			return err
		}

		err = filename.SaveJson(dbconf)
		if nil != err {
			return err
		}
	}

	return nil
}

// just for publisher/broker
func dbInit() error {
	var err error

	db, err = bolt.Open(dbFile.Abs().String(), 0755, nil)
	if nil != err {
		return err
	}

	return dbLoadConf()
}
