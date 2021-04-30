package main

import (
	"database/sql"
	"log"
	"strings"
)

func printColumns(rows *sql.Rows) {
	col, err := rows.Columns()
	onError(err)
	var sb strings.Builder
	sb.WriteString("|")
	for _, s := range col {
		sb.WriteString(s)
		sb.WriteString("|")
	}
	log.Println(sb.String())
}

func printRows(rows *sql.Rows) {

	cols, _ := rows.Columns()
	for rows.Next() {
		columns := make([]string, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		err := rows.Scan(columnPointers...)
		if err != nil {
			panic(err)
		}
		var sb strings.Builder
		sb.WriteString("|")
		for _, s := range columnPointers {
			sb.WriteString(*s.(*string))
			sb.WriteString("|")
		}
		log.Println(sb.String())
	}
}

func onError(err error) {
	if err != nil {
		log.Printf("Error %s", err)
		panic(err)
	}
}
