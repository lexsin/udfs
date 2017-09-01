package udfs

// call in main
func BrokerMain() {
	udfsInit(roleBroker)

	go udfs.listen()

	udfs.gc()
}
