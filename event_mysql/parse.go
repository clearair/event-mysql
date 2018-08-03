package event_mysql

import (
	"reflect"
	"github.com/siddontang/go-mysql/canal"
)

func findUpRow(re *canal.RowsEvent) (map[string]interface{}, map[string]interface{}) {
	row := make(map[string]interface{}, 0)
	newRow := make(map[string]interface{}, 0)
	if re.Action == canal.UpdateAction {
		for j := 0; j < len(re.Rows); j += 2 {
			for i := 0; i < len(re.Rows[j]); i ++ {
				if !reflect.DeepEqual(re.Rows[j][i], re.Rows[j+1][i]) {
					// 取出修改的值
					row[re.Table.Columns[i].Name] = re.Rows[1][i]
				}
				// 取出新行
				newRow[re.Table.Columns[i].Name] = re.Rows[1][i]
			}
		}
	}
	if re.Action == canal.InsertAction {
		for j := 0; j < len(re.Rows); j ++ {
			for i := 0; i < len(re.Rows[j]); i++ {
				if re.Rows[j][i] != nil {
					// 取出修改的值
					row[re.Table.Columns[i].Name] = re.Rows[j][i]
				}
				// 取出新行
				newRow[re.Table.Columns[i].Name] = re.Rows[j][i]
			}
		}
	}
	return row, newRow
}