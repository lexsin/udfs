package udfs

func init() {
	initEnv()
	initConf()
}

func initRole(role Role) {
	initDb(role)
	initDbConf(role)
	initFile(role)
	initEndPoint(role)
}
