package event_mysql

import (
		"github.com/siddontang/go-log/log"
	"os"
	"bytes"
	"github.com/BurntSushi/toml"
)

func savePosition(posName string, pos uint32)  error {

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)



	//e.Encode()

	f, err := os.Create(r.getMasterInfoPath())
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
