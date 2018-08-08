package rabbitmq

import (
	"event-mysql/config"
	"github.com/siddontang/go-log/log"
	"github.com/streadway/amqp"
	"github.com/vmihailenco/msgpack"
	"event-mysql/event_mysql"
)

type Message struct {
	Timestamp    int64                  `msgpack:"timestamp"`
	Action       string                 `msgpack:"action"`       // 动作 1:新增  2:修改 3:删除 4...
	Schema       string                 `msgpack:"schema"`       // 数据库
	Table        string                 `msgpack:"table"`        // 表
	ChangeFields map[string]interface{} `msgpack:"changeFields"` // 修改字段
	RawRow       map[string]interface{} `msgpack:"rawRow"`       // 原始row
	Row          map[string]interface{} `msgpack:"row"`          // 现在row
	PrimaryKeys  map[string]interface{} `msgpack:"primaryKeys"`  // 主键
}

var Conn *amqp.Connection
var Ch *amqp.Channel

func Init() {
	var err error
	// 连接
	Conn, err = amqp.Dial(config.RabbitmqCfg.Url)
	if err != nil {
		log.Fatal(err, "Failed to connect to RabbitMQ")
	}

	// 频道
	Ch, err = Conn.Channel()
	if err != nil {
		log.Fatal(err, "Failed to open a channel")
	}
}

func Publish(h *event_mysql.MyEventHandler, message Message, routingKey string) error {
	// pack
	body, err := msgpack.Marshal(&message)
	if err != nil {
		log.Error("msg pack error :", message)
	}

	// publish
	err = Ch.Publish(
		"mysql.event", // exchange
		routingKey,    // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	log.Info("route key:", routingKey)
	if err != nil {
		log.Fatal("[x] Failed to publish a message %s", message, err)
		//h.OnError("[x] Failed to publish a message %s", message, err)
	} else {
		log.Info("[√] Success to publish a message", message)
	}

	return err
}
