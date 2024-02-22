package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/gocql/gocql"
)

// User struct to hold data from Cassandra
type User struct {
	ID    string `json:"id"` // Use string type for compatibility with DynamoDB
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	// Connect to Cassandra
	cluster := gocql.NewCluster("127.0.0.1") // Adjust IP if necessary
	cluster.Keyspace = "test"
	cassandraSession, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	defer cassandraSession.Close()

	// Query Cassandra
	var users []User
	iter := cassandraSession.Query(`SELECT id, name, email FROM users`).Iter()
	var id gocql.UUID
	var name, email string
	for iter.Scan(&id, &name, &email) {
		users = append(users, User{ID: id.String(), Name: name, Email: email})
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	// Initialize AWS session for DynamoDB
	awsSession, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),             // Use your desired region
		Endpoint: aws.String("http://localhost:8000"), // DynamoDB Local endpoint
	})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}
	dynamoDBSvc := dynamodb.New(awsSession)

	// DynamoDB table name
	tableName := "UsersTable"

	// Migrate data to DynamoDB
	for _, user := range users {
		av, err := dynamodbattribute.MarshalMap(user)
		if err != nil {
			log.Fatalf("Failed to marshal user: %v", err)
		}

		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(tableName),
		}

		_, err = dynamoDBSvc.PutItem(input)
		if err != nil {
			log.Fatalf("Failed to put item into DynamoDB: %v", err)
		}
	}

	fmt.Println("Data migration completed successfully.")
}
