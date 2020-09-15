package event

import (
	"event-mysql/config"
	"event-mysql/util"
	"fmt"
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
	fmt.Println(changeFields)
	fmt.Println(row)
	fmt.Println(rawRow)
	fmt.Println(primaryKeys)

	//message := rabbitmq.Message{
	//	Timestamp:    time.Now().Unix(),
	//	Action:       event.Action,
	//	Schema:       event.Table.Schema,
	//	Table:        event.Table.Name,
	//	ChangeFields: changeFields,
	//	RawRow:       rawRow,
	//	Row:          row,
	//	PrimaryKeys:  primaryKeys,
	//}
	//rabbitmq.Publish(h, message, event.Table.Schema+"."+event.Table.Name+"."+event.Action)

	return nil
}

func (h *MyEventHandler) OnError(s string, err error) error {
	log.Fatal(s, err)
	return nil
}

func Init() {
	position.Config = canal.NewDefaultConfig()
	position.Config.Addr = config.MysqlCfg.Mysql.Host
	position.Config.User = config.MysqlCfg.Mysql.Username
	position.Config.Password = config.MysqlCfg.Mysql.Password
	position.Config.Dump.ExecutionPath = ""
	position.Config.Dump.TableDB = "aibo"

	//position.Config.ServerID = 101

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
	h := &MyEventHandler{}
	c.SetEventHandler(h)
	wg.Wrap(func() {
		position.Canal.RunFrom(*pos)
	})
	wg.Wrap(func() {
		position.savePositionLoop()
	})
	wg.Wait()
	position.Close()
}



