package utils

import "database/sql"

func GetMapSliceFromRows(rows *sql.Rows) (*[]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	mapSliceOfRows := []map[string]interface{}{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointersToValues := make([]interface{}, len(columns))
		for i, _ := range values {
			pointersToValues[i] = &values[i]
		}

		if err := rows.Scan(pointersToValues...); err != nil {
			return nil, err
		}

		mapOfRow := make(map[string]interface{})
		for i, columnName := range columns {
			val := pointersToValues[i].(*interface{})
			mapOfRow[columnName] = *val
		}
		mapSliceOfRows = append(mapSliceOfRows, mapOfRow)
	}

	return &mapSliceOfRows, nil
}
