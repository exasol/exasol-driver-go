package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/exasol/exasol-driver-go"
)

func main2() {

	fmt.Printf("Drivers=%#v\n", sql.Drivers())
	db, err := sql.Open("exasol", "exa:localhost:8563;user=sys;password=<password>")
	onError(err)

	err = db.Ping()
	onError(err)

	rows, err := db.Query("SELECT * FROM t")
	onError(err)
	printColumns(rows)
	printRows(rows)

	result, err := db.Exec(`
		INSERT INTO t
		(Name, AValue)
		VALUES('MyName', '12');`)
	onError(err)
	log.Println(result.RowsAffected())
	db.Close()
}
