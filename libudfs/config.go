package udfs

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"

	. "asdf"

	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

const (
	etcdTimeout = 3 * time.Second
	deftLive    = 15552000 // 3600*24*30*6

	minReplication  = 1
	maxReplication  = 3
	deftReplication = 2
)

const (
	UDFS_PORT = 8290

	ENV_ETCD_USER  = "APT_ETCD_USER"
	ENV_ETCD_PASS  = "APT_ETCD_PASS"
	ENV_ETCD_NODES = "APT_ETCDS"

	ENV_THIS_HOST = "THIS_HOST"
	ENV_THIS_HOME = "APT_HOME"

	ETCD_UDFS_CONFIG = "/udfs/config"
)

var (
	etcdNodes    []string // split from ENV_ETCD_NODES
	etcdNodeList string   // ENV_ETCD_NODES
	etcdUser     string   // ENV_ETCD_USER
	etcdPass     string   // ENV_ETCD_PASS
	thisHome     string   // ENV_THIS_HOME
	thisHost     string   // ENV_THIS_HOST

	thisNodeID = InvalidID
	conf       = &Conf{}
)

// udfs config
// load from etcd, when init
type Conf struct {
	Nodes       []string `json:"nodes"`
	Dirs        []string `json:"dirs"`
	Replication int      `json:"replication"`
	Port        int      `json:"port"`
	Live        Time32   `json:"live"`
}

func (me *Conf) setDefault() {
	if 0 == me.Live {
		me.Live = deftLive
	}

	if 0 == me.Port {
		me.Port = UDFS_PORT
	}

	if me.Replication < minReplication || me.Replication > maxReplication {
		// use default Replication
		me.Replication = deftReplication
	}
}

func (me *Conf) findNodeID(host string) int {
	for idx, node := range me.Nodes {
		if host == node {
			return idx
		}
	}

	return InvalidID
}

func (me *Conf) check() int {
	if 0 == len(conf.Nodes) {
		Log.Error("empty nodes")

		return StdErrError
	} else if 0 == len(conf.Dirs) {
		Log.Error("empty dirs")

		return StdErrError
	}

	count := len(me.Dirs)
	for i := 0; i < count; i++ {
		dir := FileName(me.Dirs[i])

		if !dir.DirExist() {
			Log.Error("dir: %s not exist", dir.String())

			return StdErrNoDir
		}
	}

	return 0
}

func initThisNodeID() {
	thisNodeID = conf.findNodeID(thisHost)
	if InvalidID == thisNodeID {
		Log.Error("etcd config nodes not include this-host:%s", thisHost)

		os.Exit(StdErrError)
	}
}

func getEtcd(path string, timeout time.Duration) ([]byte, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdNodes,
		DialTimeout: timeout,
		Username:    etcdUser,
		Password:    etcdPass,
	})
	if nil != err {
		Log.Error("connect etcd:%s error:%v", etcdNodeList, err)

		return nil, err
	}
	defer cli.Close()

	// get etcd path ETCD_UDFS_CONFIG
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := cli.Get(ctx, path)
	cancel()
	if err != nil {
		Log.Error("get etcd:%s error:%v", path, err)

		return nil, err
	} else if 1 != resp.Count {
		Log.Error("get etcd:%s more kvs", ETCD_UDFS_CONFIG)

		return nil, errors.New("etcd more kvs")
	} else {
		buf := resp.Kvs[0].Value

		Log.Info("get etcd:%s value:%s", ETCD_UDFS_CONFIG, string(buf))

		return buf, nil
	}
}

func initConf() {
	buf, err := getEtcd(ETCD_UDFS_CONFIG, etcdTimeout)
	if nil != err {
		os.Exit(StdErrError)
	}

	err = json.Unmarshal(buf, conf)
	if nil != err {
		Log.Error("etcd to json error:%s", err.Error())

		os.Exit(StdErrError)
	} else if errno := conf.check(); 0 != errno {
		os.Exit(errno)
	}

	conf.setDefault()
	initThisNodeID()
}

func getEnv(name string) string {
	v := os.Getenv(ENV_THIS_HOME)
	if Empty == v {
		Log.Error("no ENV:%s", ENV_THIS_HOME)

		os.Exit(StdErrNoEnv)
	}

	return v
}

func initEnv() {
	thisHome = getEnv(ENV_THIS_HOME)
	thisHost = getEnv(ENV_THIS_HOST)
	etcdNodeList = getEnv(ENV_ETCD_NODES)
	etcdUser = os.Getenv(ENV_ETCD_USER)
	etcdPass = os.Getenv(ENV_ETCD_PASS)

	if Empty == etcdNodeList {
		Log.Error("empty etcd node list")

		os.Exit(StdErrError)
	} else {
		etcdNodes = strings.Split(etcdNodeList, ",")
	}
}

func preInit() {
	initEnv()
	initConf()
}
