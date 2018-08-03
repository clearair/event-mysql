package event_mysql

import (
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
	"time"
	"event-mysql/config"
	"github.com/siddontang/go-log/log"
	"event-mysql/rabbitmq"
	"event-mysql/util"
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

var position Position


func (h *MyEventHandler) OnRotate(e *replication.RotateEvent) error {
	position.savePosition(string(e.NextLogName), uint32(e.Position))
	return nil
}
func (h *MyEventHandler) OnRow(event *canal.RowsEvent) error {
	changeFields, row, rawRow := findUpRow(event)
	message := rabbitmq.Message{
		Timestamp: time.Now().Unix(),
		Action:    1,
		Schema:    event.Table.Schema,
		Table:     event.Table.Name,
		ChangeFields: changeFields,
		RawRow: rawRow,
		Row:row,
		//PrimaryKeys: ["a"]["a"]interface{"a":1}
	}
	rabbitmq.Publish(message,"mysql." + event.Action, event.Table.Name + "." + event.Action)

	return nil
}

func Init() {
	position.Config = canal.NewDefaultConfig()
	position.Config.Addr = config.MysqlCfg.Host
	position.Config.User = config.MysqlCfg.Username
	position.Config.Password = config.MysqlCfg.Password
	position.Config.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(position.Config)
	position.Canal = c
	pos, err := position.loadPositionInfo()
	if err != nil {
		log.Fatal("new event_mysql error:", err)
	}
	wg := util.WaitGroupWrapper{}
	c.SetEventHandler(&MyEventHandler{})
	wg.Wrap(func() {
		position.Canal.RunFrom(*pos)
	})
	wg.Wrap(func() {
		position.savePositionLoop()
	})
	wg.Wait()
	position.Close()
}
