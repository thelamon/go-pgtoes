package main

import (
	"github.com/jackc/pgx"
	"log"
	"fmt"
	"time"
	"runtime"
)

type WorkRequest struct {
	id    int64
	data1 string
	data2 string
	data3 string
	data4 string
	date1 time.Time
}

var WorkQueue = make(chan *WorkRequest, 3000)

func main() {
	numCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPUs * 2)

	defer timeTrack(time.Now(), "main from start")
	connPoolConfig, err := extractConfig()
	checkErr(err)

	pgxPool, err := openPgxNative(connPoolConfig)
	checkErr(err)

	var count int
	err = pgxPool.QueryRow("SELECT COUNT(*) FROM sample_table").Scan(&count)
	checkErr(err)

	fmt.Println("sample_table rows: ", count)

	rows, err := pgxPool.Query("SELECT * FROM sample_table")
	checkErr(err)
	defer rows.Close()

	defer timeTrack(time.Now(), "main")
	StartDispatcher(4)
	startCollector(rows)
}

func startCollector(rows *pgx.Rows) {
	defer timeTrack(time.Now(), "startCollector")
	for rows.Next() {
		wr := new(WorkRequest)

		err := rows.Scan(&wr.id, &wr.data1, &wr.data2, &wr.data3, &wr.data4, &wr.date1)
		checkErr(err)

		WorkQueue <- wr
	}

}

func checkErr(err error) {
	if err != nil {
		log.Fatal("ERROR: ", err)
	}
}

func extractConfig() (config pgx.ConnPoolConfig, err error) {
	config.ConnConfig, err = pgx.ParseEnvLibpq()
	if err != nil {
		return config, err
	}

	if config.Host == "" {
		config.Host = "localhost"
	}

	if config.User == "" {
		config.User = "gotest"
	}

	if config.Password == "" {
		config.Password = "gotest"
	}

	if config.Database == "" {
		config.Database = "gotest"
	}

	config.TLSConfig = nil
	config.UseFallbackTLS = false

	config.MaxConnections = 10

	return config, nil
}

func openPgxNative(config pgx.ConnPoolConfig) (*pgx.ConnPool, error) {
	return pgx.NewConnPool(config)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
