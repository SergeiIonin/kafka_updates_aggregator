package main

import (
	"github.com/gin-gonic/gin"
	"kafka_updates_aggregator/restapi/application"
	"kafka_updates_aggregator/restapi"
	"net/http"
)

func main() {
	schemaRegistryUrl := "http://localhost:8081"
	aggregationSchemasService := application.NewSchemaAggregationService(schemaRegistryUrl)

	rc := restapi.NewRestController(aggregationSchemasService)
	router := gin.Default()
	router.LoadHTMLGlob("templates/*")
	router.GET("/favicon.ico", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	router.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/start")
	})
	router.GET("/start", rc.Start)
	router.POST("/create", rc.CreateSchema)
	router.GET("/schemas", rc.GetSchemas)

	router.Run("localhost:8082")
}
