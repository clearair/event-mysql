package event_mysql

import (
	"event-mysql/config"
	"event-mysql/rabbitmq"
	"event-mysql/util"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
	"time"
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
	changeFields, row, rawRow, primaryKeys := findUpRow(event)
	message := rabbitmq.Message{
		Timestamp:    time.Now().Unix(),
		Action:       event.Action,
		Schema:       event.Table.Schema,
		Table:        event.Table.Name,
		ChangeFields: changeFields,
		RawRow:       rawRow,
		Row:          row,
		PrimaryKeys:  primaryKeys,
	}
	rabbitmq.Publish(message, event.Table.Schema+"."+event.Table.Name+"."+event.Action)

	return nil
}

func Init() {
	position.Config = canal.NewDefaultConfig()
	position.Config.Addr = config.MysqlCfg.Host
	position.Config.User = config.MysqlCfg.Username
	position.Config.Password = config.MysqlCfg.Password
	position.Config.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(position.Config)
	if err != nil {
		log.Fatal("new event_mysql error:", err)
	}

	position.Canal = c
	pos, err := position.loadPositionInfo()
	if err != nil {
		log.Fatal("load position info error:", err)
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
