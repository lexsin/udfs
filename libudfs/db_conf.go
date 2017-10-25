package udfs

import (
	. "asdf"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
)

var dbConf = &DbConf{}
var dbConfOld = &DbConf{}

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

func initDbConf(role Role) error {
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
	} else if role != roleConsumer {
		// db config NOT exist, load it from etcd
		dbConf.dirs = conf.Dirs

		err := filename.SaveJson(dbConf)
		if nil != err {
			return err
		}
	}

	return nil
}
