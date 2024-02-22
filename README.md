# Cassandra to DynamoDB Data Migration

This project demonstrates a data migration pipeline that efficiently transfers large datasets from CassandraDB to Amazon DynamoDB using Go. It includes scripts for populating Cassandra with a large dataset, migrating this data to DynamoDB, and handling batch operations and error retries for robustness.

## Features

- **Data Population**: A Go script to generate and insert a large dataset into CassandraDB, simulating a real-world user data model.
- **Efficient Migration**: Leveraging Go for batch fetching from CassandraDB and batch writing to DynamoDB, optimizing throughput and minimizing API throttling.
- **Error Handling**: Implements comprehensive error management, including retries with exponential backoff, ensuring data integrity during the migration process.
- **Scalability**: Designed to handle large datasets, demonstrating practices for working with high-volume data in distributed databases.

## Getting Started

### Prerequisites

- CassandraDB setup and running.
- DynamoDB (local or AWS managed) setup.
- Go installed on your machine.
- AWS CLI configured (for DynamoDB).

### Installation

1. **Clone the repository**

    ```bash
    git clone https://github.com/skoko1/cassandradbtodynamodbproj.git
    cd cassandradbtodynamodbproj
    ```

2. **Install Go dependencies**

    Navigate to the project directory and install the required Go modules:

    ```bash
    go mod tidy
    ```

### Usage

1. **Populate CassandraDB**

    Run the `populate_cassandra.go` script to insert dummy data into your CassandraDB:

    ```bash
    go run cmd/populate/main.go
    ```

2. **Migrate Data to DynamoDB**

    Execute the `migrate.go` script to transfer data from CassandraDB to DynamoDB:

    ```bash
    go run cmd/migrate/main.go
    ```

    Monitor the output for progress and any potential errors.

## Configuration

- Adjust Cassandra and DynamoDB settings in the Go scripts as per your environment setup.
- Customize the data model and migration logic in the scripts according to your requirements.

## Contributing

Contributions are welcome! Please feel free to submit pull requests, open issues, or suggest improvements.

## License

Distributed under the MIT License. See `LICENSE` for more information.


Project Link: [https://github.com/skoko1/cassandradbtodynamodbproj](https://github.com/skoko1/cassandradbtodynamodbproj)

