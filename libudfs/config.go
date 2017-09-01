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
	etcdTimeout     = 3 * time.Second
	minReplication  = 1
	maxReplication  = 3
	deftReplication = 2

	UDFS_PORT = 8290

	ENV_ETCD_USER  = "APT_ETCD_USER"
	ENV_ETCD_PASS  = "APT_ETCD_PASS"
	ENV_ETCD_NODES = "APT_ETCDS"

	ENV_THIS_HOST = "THIS_HOST"
	ENV_THIS_HOME = "APT_HOME"

	ETCD_UDFS_CONFIG = "/udfs/config"
)

var (
	etcdNodes    []string
	etcdNodeList string
	etcdUser     string
	etcdPass     string
	thisHome     string
	thisHost     string

	thisNodeID = -1
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

func getEtcd(path string, timeout time.Duration) ([]byte, error) {
	// new etcd client
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

		return nil, errors.New("")
	}

	var buf []byte
	for _, v := range resp.Kvs {
		buf = v.Value
	}

	return buf, nil
}

func initConf() {
	buf, err := getEtcd(ETCD_UDFS_CONFIG, etcdTimeout)
	if nil != err {
		os.Exit(StdErrError)
	}

	err = json.Unmarshal(buf, conf)
	if nil != err {
		Log.Error("get etcd json config error:%v", err)

		os.Exit(StdErrError)
	} else if nil == conf.Nodes || 0 == len(conf.Nodes) {
		Log.Error("get etcd json config error:empty nodes")

		os.Exit(StdErrError)
	} else if nil == conf.Dirs || 0 == len(conf.Dirs) {
		Log.Error("get etcd json config error:empty dirs")

		os.Exit(StdErrError)
	} else if 0 == conf.Port {
		Log.Error("get etcd json config error:bad port")

		os.Exit(StdErrError)
	} else if 0 == conf.Live {
		Log.Error("get etcd json config error:bad port")

		os.Exit(StdErrError)
	} else if 0 == conf.Port {
		conf.Port = UDFS_PORT
	} else if conf.Replication < minReplication || conf.Replication > maxReplication {
		conf.Replication = deftReplication
	}

	for idx, node := range conf.Nodes {
		if thisHost == node {
			thisNodeID = idx
			break
		}
	}

	if -1 == thisNodeID {
		Log.Error("etcd config nodes not include this-host:%s", thisHost)

		os.Exit(StdErrError)
	}
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

	etcdNodes = strings.Split(etcdNodeList, ",")
}

func preInit() {
	initEnv()
	initConf()
}
