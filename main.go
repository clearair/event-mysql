package main

import (
	"event-mysql/config"
	"event-mysql/event"
)

func main()  {
	config.Init()
	event.Init()
}
