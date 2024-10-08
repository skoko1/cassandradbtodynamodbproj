package main

import (
    "fmt"
    "log"
    "math"
    "time"

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
    // Maximum number of retries for operations
    const maxRetries = 5

    // Connect to Cassandra with retries
    var cassandraSession *gocql.Session
    err := retryWithExponentialBackoff(maxRetries, func() error {
        var err error
        cassandraSession, err = connectToCassandra()
        return err
    })
    if err != nil {
        log.Fatalf("Failed to connect to Cassandra after retries: %v", err)
    }
    defer cassandraSession.Close()

    // Query Cassandra with retries
    var users []User
    err = retryWithExponentialBackoff(maxRetries, func() error {
        var err error
        users, err = queryCassandra(cassandraSession)
        return err
    })
    if err != nil {
        log.Fatalf("Failed to query Cassandra after retries: %v", err)
    }

    // Initialize AWS session for DynamoDB with retries
    var dynamoDBSvc *dynamodb.DynamoDB
    err = retryWithExponentialBackoff(maxRetries, func() error {
        var err error
        dynamoDBSvc, err = createDynamoDBSession()
        return err
    })
    if err != nil {
        log.Fatalf("Failed to create AWS session after retries: %v", err)
    }

    // DynamoDB table name
    tableName := "UsersTable"

    // Migrate data to DynamoDB with retries
    for _, user := range users {
        err = retryWithExponentialBackoff(maxRetries, func() error {
            return putItemToDynamoDB(dynamoDBSvc, tableName, user)
        })
        if err != nil {
            log.Fatalf("Failed to put item into DynamoDB after retries: %v", err)
        }
    }

    fmt.Println("Data migration completed successfully.")
}

// connectToCassandra attempts to establish a session with Cassandra
func connectToCassandra() (*gocql.Session, error) {
    cluster := gocql.NewCluster("127.0.0.1") // Adjust IP if necessary
    cluster.Keyspace = "test"
    session, err := cluster.CreateSession()
    if err != nil {
        return nil, fmt.Errorf("failed to create Cassandra session: %w", err)
    }
    return session, nil
}

// queryCassandra retrieves users from Cassandra
func queryCassandra(session *gocql.Session) ([]User, error) {
    var users []User
    iter := session.Query(`SELECT id, name, email FROM users`).Iter()
    var id gocql.UUID
    var name, email string
    for iter.Scan(&id, &name, &email) {
        users = append(users, User{ID: id.String(), Name: name, Email: email})
    }
    if err := iter.Close(); err != nil {
        return nil, fmt.Errorf("failed to close Cassandra iterator: %w", err)
    }
    return users, nil
}

// createDynamoDBSession initializes an AWS session for DynamoDB
func createDynamoDBSession() (*dynamodb.DynamoDB, error) {
    awsSession, err := session.NewSession(&aws.Config{
        Region:   aws.String("us-west-2"),             // Use your desired region
        Endpoint: aws.String("http://localhost:8000"), // DynamoDB Local endpoint
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create AWS session: %w", err)
    }
    return dynamodb.New(awsSession), nil
}

// putItemToDynamoDB inserts a user into DynamoDB
func putItemToDynamoDB(svc *dynamodb.DynamoDB, tableName string, user User) error {
    av, err := dynamodbattribute.MarshalMap(user)
    if err != nil {
        return fmt.Errorf("failed to marshal user: %w", err)
    }

    input := &dynamodb.PutItemInput{
        Item:      av,
        TableName: aws.String(tableName),
    }

    _, err = svc.PutItem(input)
    if err != nil {
        return fmt.Errorf("failed to put item into DynamoDB: %w", err)
    }

    return nil
}

// retryWithExponentialBackoff retries a function with exponential backoff
func retryWithExponentialBackoff(maxRetries int, fn func() error) error {
    var err error
    for attempt := 0; attempt <= maxRetries; attempt++ {
        err = fn()
        if err == nil {
            // Operation succeeded
            return nil
        }

        // Log the error and prepare to retry
        waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
        log.Printf("Attempt %d failed: %v. Retrying in %v...", attempt+1, err, waitTime)

        // Wait before retrying
        time.Sleep(waitTime)
    }
    // All retries failed
    return fmt.Errorf("after %d attempts, last error: %w", maxRetries+1, err)
}
