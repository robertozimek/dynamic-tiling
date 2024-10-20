package postgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net/url"
)

type DBConnectionOptions struct {
	Host     string
	Port     string
	User     string
	Pass     string
	Database string
	SSLMode  string
	ReadOnly bool
}

func NewDBConnection(options *DBConnectionOptions) *sql.DB {
	port := options.Port
	if port == "" {
		port = "5432"
	}

	pass := url.QueryEscape(options.Pass)

	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", options.User, pass, options.Host, port, options.Database, options.SSLMode)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}

	if options.ReadOnly {
		_, err := db.Exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
		if err != nil {
			panic(err)
		}
	}

	return db
}
