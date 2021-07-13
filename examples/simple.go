package main

import (
	"database/sql"
	"fmt"
	"github.com/exasol/exasol-driver-go"
	"log"
)

func main() {
	fmt.Printf("Drivers=%#v\n", sql.Drivers())
	database, err := sql.Open("exasol", exasol.NewConfig("<username>", "<password>").Host("<host>").Port(8563).String())
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
