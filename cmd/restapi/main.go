package main

import (
	"fmt"
	"kafka_updates_aggregator/configs"
	"kafka_updates_aggregator/restapi"
	"kafka_updates_aggregator/restapi/application"
	"log"
	"net/http"
	"os"
	restapiconfig "kafka_updates_aggregator/configs/restapi"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

var (
	configTplPath string
	valuesPath    string
	configPath    string
	configFile    []byte
)

func init() {
	pwd, _ := os.Getwd()
	basePath := fmt.Sprintf("%s/configs/restapi", pwd)

	configTplPath = configs.GetPath(basePath, "templates/restapi_config_template.yaml")
	valuesPath = configs.GetPath(basePath, "values.yaml")
	configPath = configs.GetPath(basePath, "restapi_config.yaml")

	configReader := configs.NewConfigReader()

	err := configReader.ReadConfig(configTplPath, valuesPath, configPath)
	if err != nil {
		log.Fatalf("Error creating restapi_config: %v", err)
	}
	
	configFile, err = os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading restapi config file: %v", err)
	}
}

func main() {
	var restapiConfig restapiconfig.RestApiConfig

	err := yaml.Unmarshal(configFile, &restapiConfig)
	if err != nil {
		log.Fatalf("Error parsing restapiConfig file: %v", err)
	}

	conf := restapiConfig.RestApi

	schemaRegistryUrl := conf.SchemaRegistryAddress
	address := fmt.Sprintf("%s:%d", conf.Host, conf.Port)

	aggregationSchemasService := application.NewSchemaAggregationService(schemaRegistryUrl)

	rc := restapi.NewRestController(aggregationSchemasService)
	router := gin.Default()
	router.GET("/favicon.ico", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	router.POST("/create", rc.CreateSchema)
	router.GET("/schemas", rc.GetSchemas)

    err = router.Run(address)
    if err != nil {
        panic(err)
    }
}
