package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/exasol/exasol-driver-go"
)

func main() {
	fmt.Printf("Drivers=%#v\n", sql.Drivers())
	database, err := sql.Open("exasol", exasol.NewConfig("sys", "exasol").
		Host("localhost").
		Port(8563).
		ValidateServerCertificate(false).
		String())

	onError(err)
	defer database.Close()

	err = database.Ping()
	onError(err)

	_, err = database.Exec("CREATE SCHEMA IF NOT EXISTS my_schema")
	onError(err)

	_, err = database.Exec("OPEN SCHEMA my_schema")
	onError(err)

	_, err = database.Exec("CREATE OR REPLACE TABLE my_schema.CUSTOMERS (ref_id int , b VARCHAR(20)) ")
	onError(err)

	result, err := database.Exec(`IMPORT INTO my_schema.CUSTOMERS FROM LOCAL CSV FILE './data.csv' COLUMN SEPARATOR = ';' ENCODING = 'UTF-8'
ROW SEPARATOR = 'LF'`)
	onError(err)
	log.Println(result.RowsAffected())
	rows, err := database.Query("SELECT * FROM my_schema.CUSTOMERS")
	onError(err)
	defer rows.Close()

	printColumns(rows)
	printRows(rows)
}
