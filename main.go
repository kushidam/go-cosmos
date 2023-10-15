package main

import (
	"go-cosmos/config"
	"go-cosmos/cosmosdb"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

var (
	cosmosDbPrimaryKey string
	cosmosURIEndpoint  string
	cosmosDbName       string
	containerName      string
	partitionKey       string
)

func init() {
	cosmosDbPrimaryKey = config.CosmosDbKey
	cosmosURIEndpoint = config.CosmosDbEndpoint
	cosmosDbName = config.DatabaseName
	containerName = config.ContainerName
	partitionKey = config.PartitionKey

}

func handle(err error) {
	if err != nil {
		log.Println(err)
	}
}

func main() {
	item := struct {
		ID           string `json:"id"`
		CustomerId   string `json:"customerId"`
		Title        string
		UserName     string
		EmailAddress string
		PhoneNumber  string
		CreationDate string
	}{
		ID:           "1",
		CustomerId:   "1",
		Title:        "Mr",
		UserName:     "testUser",
		EmailAddress: "test@example.com",
		PhoneNumber:  "123-456-7890",
	}

	cred, err := azcosmos.NewKeyCredential(cosmosDbPrimaryKey)
	if err != nil {
		log.Fatal("Failed to create a credential: ", err)
	}

	// Create a CosmosDB client
	client, err := azcosmos.NewClientWithKey(cosmosURIEndpoint, cred, nil)
	if err != nil {
		log.Fatal("Failed to create Azure Cosmos DB db client: ", err)
	}

	err = cosmosdb.CreateDatabase(client, cosmosDbName)
	if err != nil {
		log.Printf("createDatabase failed: %s\n", err)
	}

	err = cosmosdb.CreateContainer(client, cosmosDbName, containerName, partitionKey)
	if err != nil {
		log.Printf("createContainer failed: %s\n", err)
	}

	// ここからデータベース操作などを行う
	err = cosmosdb.CreateItem(client, cosmosDbName, containerName, item.CustomerId, item)
	if err != nil {
		log.Printf("createItem failed: %s\n", err)
	}

	err = cosmosdb.ReadItem(client, cosmosDbName, containerName, item.CustomerId, item.ID)
	if err != nil {
		log.Printf("readItem failed: %s\n", err)
	}

	err = cosmosdb.DeleteItem(client, cosmosDbName, containerName, item.CustomerId, item.ID)
	if err != nil {
		log.Printf("deleteItem failed: %s\n", err)
	}

}
