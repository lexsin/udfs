package udfs

import (
	. "asdf"
	"fmt"
)

var dirLocks []*RwLock

func newDirLocks() []*RwLock {
	count := len(conf.Dirs)
	locks := make([]*RwLock, count)

	for i := 0; i < count; i++ {
		locks[i] = NewRwLock(fmt.Sprintf("dir[%d]-locker", i), false)
	}

	return locks
}

func fileInit() {
	dirLocks = newDirLocks()
}

type UdfsFile struct {
	name FileName
	idir int
}

func (me *UdfsFile) String() string {
	return me.name.String()
}

func (me *UdfsFile) rhandle(handle func() error) error {
	var err error

	dirLocks[me.idir].RHandle(func() {
		err = handle()
	})

	return err
}

func (me *UdfsFile) whandle(handle func() error) error {
	var err error

	dirLocks[me.idir].WHandle(func() {
		err = handle()
	})

	return err
}

func (me *UdfsFile) Delete() error {
	err := me.whandle(func() error {
		return me.name.Delete()
	})
	if nil != err {
		Log.Error("delete fils:%s error:%v", me.String(), err.Error())
	}

	return err
}

func (me *UdfsFile) Save(buf []byte) error {
	err := me.whandle(func() error {
		return me.name.Save(buf)
	})
	if nil != err {
		Log.Error("save fils:%s error:%v", me.String(), err.Error())
	}

	return err
}

func (me *UdfsFile) Touch(Time Time32) error {
	err := me.whandle(func() error {
		return me.name.Touch(Time)
	})
	if nil != err {
		Log.Error("touch fils:%s error:%v", me.String(), err.Error())
	}

	return err
}

func (me *UdfsFile) Exist() bool {
	exist := false

	me.rhandle(func() error {
		exist = me.name.Exist()

		return nil
	})

	return exist
}
