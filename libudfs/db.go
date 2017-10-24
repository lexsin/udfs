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

var db *bolt.DB
var dbConf = &DbConf{}
var dbConfOld = &DbConf{}

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

type DbConf struct {
	dirs []string `json:"dirs"`
}

func (me *DbConf) idir(bkdr Bkdr) byte {
	return byte(bkdr % Bkdr(len(me.dirs)))
}

func (me *DbConf) path(bkdr Bkdr) UdfsFile {
	var b [4]byte
	var s [8]byte

	binary.BigEndian.PutUint32(b[:], uint32(bkdr))
	hex.Encode(s[:], b[:])

	idir := me.idir(bkdr)
	path := filepath.Join(dbConf.dirs[idir], string(s[0:4]), string(s[4:8]))

	return UdfsFile{
		name: FileName(path),
		idir: int(idir),
	}
}

func (me *DbConf) file(path UdfsFile, digest []byte) UdfsFile {
	file := filepath.Join(path.String(), hex.EncodeToString(digest))

	return UdfsFile{
		name: FileName(file),
		idir: path.idir,
	}
}

func (me *DbConf) File(bkdr Bkdr, digest []byte) UdfsFile {
	return me.file(me.path(bkdr), digest)
}

func (me *DbConf) eq() bool {
	if len(me.dirs) != len(conf.Dirs) {
		return false
	}

	for i, dir := range me.dirs {
		if dir != conf.Dirs[i] {
			return false
		}
	}

	return true
}

const sizeofDbEntry = SizeofByte + 2*SizeofInt32 + DigestSize

type DbEntry struct {
	time   Time32
	bkdr   Bkdr
	idir   byte
	digest [DigestSize]byte
}

func dbBucket(bkdr Bkdr) []byte {
	bucket := [2]byte{}

	binary.BigEndian.PutUint16(bucket[:], uint16(bkdr))

	return bucket[:]
}

func (me *DbEntry) String() string {
	return fmt.Sprintf("time:%v bkdr:%x dir:%d digest:%s",
		me.time.Unix(),
		me.bkdr,
		me.idir,
		hex.EncodeToString(me.digest[:]))
}

const DbEntrySize = SizeofByte + 2*SizeofInt32 + DigestSize

func (me *DbEntry) Size() int {
	return DbEntrySize
}

func (me *DbEntry) ToBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	binary.BigEndian.PutUint32(bin[0:], uint32(me.time))
	binary.BigEndian.PutUint32(bin[4:], uint32(me.bkdr))
	bin[8] = byte(me.idir)

	copy(bin[9:], me.digest[:])

	return nil
}

func (me *DbEntry) FromBinary(bin []byte) error {
	if len(bin) < me.Size() {
		return ErrTooShortBuffer
	}

	me.time = Time32(binary.BigEndian.Uint32(bin[0:]))
	me.bkdr = Bkdr(binary.BigEndian.Uint32(bin[4:]))
	me.idir = bin[8]

	copy(me.digest[:], bin[9:])

	return nil
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

func dbLoadConf() error {
	filename := conf.DbConfName.Abs()
	if filename.Exist() {
		// db config exist, load it
		err := filename.LoadJson(dbConf)
		if nil != err {
			return err
		}

		if !dbConf.eq() {
			// self is broker
			// db config != etcd config
			// disk load balance
			if err = dbDiskLoadBalance(); nil != err {
				return err
			}
		}
	} else {
		// db config NOT exist, load it from etcd
		dbConf.dirs = conf.Dirs

		err := filename.SaveJson(dbConf)
		if nil != err {
			return err
		}
	}

	return nil
}

// just for publisher/broker
func dbInit() error {
	var err error

	db, err = bolt.Open(conf.DbFileName.Abs().String(), 0755, nil)
	if nil != err {
		return err
	}

	return dbLoadConf()
}
