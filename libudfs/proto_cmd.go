package udfs

type ProtoCmd byte

const (
	cmdPush  ProtoCmd = 0 // publisher ==> leader ==> follower
	cmdTouch ProtoCmd = 1 // publisher ==> leader ==> follower
	cmdPull  ProtoCmd = 2 // consumer  ==> leader ==> follower
	cmdDel   ProtoCmd = 3 // gc
	cmdEnd   ProtoCmd = 4
)

var cmdStrings = [cmdEnd]string{
	cmdPush:  "push",
	cmdTouch: "touch",
	cmdPull:  "pull",
	cmdDel:   "del",
}

func (me ProtoCmd) IsGood() bool {
	return me >= 0 && me < cmdEnd
}

func (me ProtoCmd) String() string {
	return cmdStrings[me]
}
