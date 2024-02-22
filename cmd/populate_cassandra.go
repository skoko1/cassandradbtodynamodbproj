package main

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	for i := 0; i < 100000; i++ { // Adjust the number of inserts as needed
		if err := session.Query(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`,
			gocql.TimeUUID(), fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i)).Exec(); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Finished inserting dummy data into Cassandra.")
}
