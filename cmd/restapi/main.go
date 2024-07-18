package main

import (
    "github.com/gin-gonic/gin"
    "kafka_updates_aggregator/restapi"
    "kafka_updates_aggregator/restapi/application"
    "net/http"
)

func main() {
	schemaRegistryUrl := "http://localhost:8081"
	aggregationSchemasService := application.NewSchemaAggregationService(schemaRegistryUrl)

	rc := restapi.NewRestController(aggregationSchemasService)
	router := gin.Default()
	router.GET("/favicon.ico", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	router.POST("/create", rc.CreateSchema)
	router.GET("/schemas", rc.GetSchemas)

    err := router.Run("localhost:8082")
    if err != nil {
        panic(err)
    }
}
