package event_mysql

import (
	"github.com/siddontang/go-log/log"
	"os"
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/siddontang/go-mysql/mysql"
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"errors"
	"sync"
	"time"
)

type Position struct {
	Config *canal.Config
	Canal  *canal.Canal
	mutex sync.Mutex
	pos *MysqlPos
	exitChan chan struct{}
}

type MysqlPos struct {
	Addr string `toml:"addr"`
	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`
}

func (p *Position) savePosition(posName string, pos uint32) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.pos == nil {
		p.pos = &MysqlPos{
			Addr: p.Config.Addr,
			Name: posName,
			Pos:  pos,
		}
	} else {
		p.pos.Name = posName
		p.pos.Pos = pos
	}
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	e.Encode(map[string]interface{}{"name": posName, "pos": pos})

	f, err := os.Create(p.getPositionInfoPath())
	if err != nil {
		log.Warnf("create master info file error - %s", err)
		return err
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		log.Warnf("save master info to file  error - %s", err)
		return err
	}

	log.Debug("save binlog position succ")
	return nil
}

func (p *Position) getPositionInfoPath() string {
	return "./position.info"
}

func (p *Position) loadPositionInfo() (*mysql.Position, error) {
	f, err := os.Open(p.getPositionInfoPath())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
		//文件不存在,默认从最新的位置开始
		return p.getNewestPos()
	}

	defer f.Close()

	var mysqlPos MysqlPos
	_, err = toml.DecodeReader(f, &mysqlPos)
	if err != nil || mysqlPos.Addr != p.Config.Addr || mysqlPos.Name == "" {
		return p.getNewestPos()
	}

	return &mysql.Position{mysqlPos.Name, mysqlPos.Pos}, nil
}

func (p *Position) getNewestPos() (*mysql.Position, error) {
	result, err := p.Canal.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, fmt.Errorf("show master status error - %s", err)
	}

	if result.Resultset.RowNumber() != 1 {
		return nil, errors.New("select master info error")
	}

	binlogName, _ := result.GetStringByName(0, "File")
	binlogPos, _ := result.GetIntByName(0, "Position")

	log.Infof("fetch mysql(%s)'s the newest pos:(%s, %d)", p.Config.Addr, binlogName, binlogPos)

	return &mysql.Position{binlogName, uint32(binlogPos)}, nil
}

func (p *Position) savePositionLoop() {
	ticker := time.NewTicker(2 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			pos := position.Canal.SyncedPosition()
			if p.pos == nil || pos.Name != p.pos.Name || pos.Pos != p.pos.Pos {
				err := p.savePosition(pos.Name, pos.Pos)
				if err != nil {
					log.Warnf("save binlog position error from per second - %s", err)
				}
			}

		case <-p.exitChan:
			log.Info("save binlog position loop exit.")
			return
		}
	}

}

//Close 关闭Rail,释放资源
func (p *Position) Close() {
	//关闭canal
	p.Canal.Close()

	//save binlog postion
	pos := p.Canal.SyncedPosition()
	err := p.savePosition(pos.Name, pos.Pos)
	if err != nil {
		log.Warnf("save binlog position error when closing - %s", err)
	}

	//关闭topic
	//err = p.topic.Close()
	//if err != nil {
	//	log.Errorf("TOPIC(%s): close fail - %s", r.topic.name, err)
	//}

	close(p.exitChan)

	//p.waitGroup.Wait()

	log.Info("rail safe close.")
}