package event_mysql

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
		"time"
	"event-mysql/config"
		"github.com/siddontang/go-log/log"
	"event-mysql/rabbitmq"
)

type MyEventHandler struct {
	canal.DummyEventHandler
}

type EventLog struct {
	Id      int64
	Name    string
	Pos     uint32
	Created time.Time
}



func (h *MyEventHandler) OnRotate(e *replication.RotateEvent) error {
	savePosition(string(e.NextLogName), uint32(e.Position))
	return nil
}
func (h *MyEventHandler) OnRow(event *canal.RowsEvent) error {
	rawRow, row := findUpRow(event)
	message := rabbitmq.Message{
		Timestamp: time.Now().Unix(),
		Action:    1,
		Schema:    event.Table.Schema,
		Table:     event.Table.Name,
		ChangeFields: map[string]interface{}{"a":1},
		RawRow: rawRow,
		Row:row,
		//PrimaryKeys: ["a"]["a"]interface{"a":1}
	}
	rabbitmq.Publish(message,"opentrust","staff-opentrust")

	return nil
}

func Init() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.MysqlCfg.Host
	cfg.User = config.MysqlCfg.Username
	cfg.Password = config.MysqlCfg.Password
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal("new event_mysql error:", err)
	}
	c.SetEventHandler(&MyEventHandler{})
	c.Run()
}
