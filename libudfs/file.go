package udfs

import (
	. "asdf"
	"fmt"
)

var fileLock = NewRwLock("udfs-file-lock", false)

var locks []*RwLock

type udfsFile struct {
	name FileName
	idir byte
}

func fileInit() {
	count := len(conf.Dirs)
	locks = make([]*RwLock, count)

	for i := 0; i < count; i++ {
		locks[i] = NewRwLock(fmt.Sprintf("dir[%d]-lock", i), false)
	}
}

func (me *udfsFile) String() string {
	return me.name.String()
}

func (me *udfsFile) rlock(handle func() error) error {
	var err error

	locks[me.idir].RHandle(func() {
		err = handle()
	})

	return err
}

func (me *udfsFile) wlock(handle func() error) error {
	var err error

	locks[me.idir].WHandle(func() {
		err = handle()
	})

	return err
}

func (me *udfsFile) Delete() error {
	err := me.wlock(func() error {
		return me.name.Delete()
	})
	if nil != err {
		Log.Error("delete fils:%s error:%v", me.String(), err.Error())
	}

	return err
}

func (me *udfsFile) Save(buf []byte) error {
	err := me.wlock(func() error {
		return me.name.Save(buf)
	})
	if nil != err {
		Log.Error("save fils:%s error:%v", me.String(), err.Error())
	}

	return err
}

func (me *udfsFile) Touch(Time Time32) error {
	err := me.wlock(func() error {
		return me.name.Touch(Time)
	})
	if nil != err {
		Log.Error("touch fils:%s error:%v", me.String(), err.Error())
	}

	return err
}

func (me *udfsFile) Exist() bool {
	exist := false

	me.rlock(func() error {
		exist = me.name.Exist()

		return nil
	})

	return exist
}
