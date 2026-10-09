// This command prints a reviewable index plan. It never applies DDL.
package main

import (
	"fmt"
	"log"
	"os"
	"watchAlert/config"
	"watchAlert/internal/perf"
	"watchAlert/pkg/client"
)

func main() {
	config.InitConfig("index-plan")
	c := config.Application.Database
	if c.Type == "sqlite" {
		if c.Path == "" {
			c.Path = "data/watchalert.db"
		}
		if _, err := os.Stat(c.Path); err != nil {
			log.Fatal("SQLite database must already exist; no database will be created")
		}
	}
	db, err := client.OpenDB(client.DBConfig{Type: c.Type, Host: c.Host, Port: c.Port, User: c.User, Pass: c.Pass, DBName: c.DBName, Timeout: c.Timeout, Path: c.Path})
	if err != nil {
		log.Fatal("Cannot connect to database; verify local configuration and network access")
	}
	conn, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	plan, err := perf.PlanIndexes(db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("-- Review schema, equivalent existing indexes, free space and load before running any DDL. No DDL has been applied.")
	for _, line := range plan {
		fmt.Println(line)
	}
}
