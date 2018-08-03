package main

import (
		"event-mysql/config"
	"event-mysql/event_mysql"
	"event-mysql/rabbitmq"
)

func main() {
	config.Init()
	rabbitmq.Init()
	event_mysql.Init()
}


