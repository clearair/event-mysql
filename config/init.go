package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"os"
)

type MysqlInit struct {
	Host string `yaml: "host"`
	//Port uint16 `yaml: "port"`
	Username string `yaml: "username"`
	Password string `yaml: "password"`
}

type Config struct {
	Mysql MysqlInit `yaml: ",mysql"`
}

var MysqlCfg Config

func Init()  {
	file, err := os.Open("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	yaml.NewDecoder(file).Decode(&MysqlCfg)

	fmt.Print(MysqlCfg)

}