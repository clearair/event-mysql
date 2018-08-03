package config

import (
	"github.com/micro/go-config"
	"github.com/micro/go-config/source/file"
	"log"
)

type Mysql struct {
	Host     string      `json:"host"`
	Username string      `json:"username"`
	Password string `json:"password"`
}
type Rabbitmq struct {
	Url     string      `json:"url"`
}

var MysqlCfg Mysql
var RabbitmqCfg Rabbitmq

func Init() {
	config.Load(file.NewSource(
		file.WithPath("config.yaml"),
	))

	if err := config.Get("mysql").Scan(&MysqlCfg); err != nil {
		log.Print("load mysql config error:", err)
	}

	if err := config.Get("rabbitmq").Scan(&RabbitmqCfg); err != nil {
		log.Print("load rabbitmq config error:", err)
	}
}
