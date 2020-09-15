package event


import (
	"github.com/siddontang/go-mysql/canal"
	"reflect"
	"strconv"
)

func findUpRow(re *canal.RowsEvent) (map[string]interface{}, map[string]interface{}, map[string]interface{}, map[string]interface{}) {
	row := make(map[string]interface{}, 0)
	newRow := make(map[string]interface{}, 0)
	rawRow := make(map[string]interface{}, 0)
	if re.Action == canal.UpdateAction {
		for j := 0; j < len(re.Rows); j += 2 {
			for i := 0; i < len(re.Rows[j]); i++ {
				if !reflect.DeepEqual(re.Rows[j][i], re.Rows[j+1][i]) {
					// 取出修改的值
					row[re.Table.Columns[i].Name] = re.Rows[1][i]
				}
				rawRow[re.Table.Columns[i].Name] = re.Rows[0][i]
				// 取出新行
				newRow[re.Table.Columns[i].Name] = re.Rows[1][i]
			}
		}
	}
	if re.Action == canal.InsertAction {
		for j := 0; j < len(re.Rows); j++ {
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

	deleteMap := make(map[string]interface{}, 0)
	if re.Action == canal.DeleteAction {
		for i := 0; i < len(re.Rows); i++ {
			for j := 0; j < len(re.Rows[i]); j++ {
				deleteMap[re.Table.Columns[j].Name] = re.Rows[i][j]
			}
			rawRow[strconv.Itoa(i)] = deleteMap
		}
	}

	// 主键
	pk := make(map[string]interface{}, len(re.Table.PKColumns))
	for range re.Rows {
		for _, pkIndex := range re.Table.PKColumns {
			column := re.Table.Columns[pkIndex].Name

			// 新增主键问题修改
			if re.Action == canal.InsertAction {
				pk[column] = row[column]
			} else {
				pk[column] = rawRow[column]
			}

		}
	}

	return row, newRow, rawRow, pk
}
