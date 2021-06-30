package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/exasol/exasol-driver-go"
)

func main() {
	fmt.Printf("Drivers=%#v\n", sql.Drivers())
	database, err := sql.Open("exasol", "exa:localhost:8563;user=sys;password=<password>")
	onError(err)
	defer database.Close()

	err = database.Ping()
	onError(err)

	rows, err := database.Query("SELECT * FROM CUSTOMERS")
	onError(err)
	defer rows.Close()

	printColumns(rows)
	printRows(rows)

	result, err := database.Exec(`
		INSERT INTO CUSTOMERS
		(NAME, CITY)
		VALUES('Bob', 'Berlin');`)
	onError(err)
	log.Println(result.RowsAffected())
}
