package restapi

import (
	"fmt"
	"kafka_updates_aggregator/restapi/application"
	"kafka_updates_aggregator/restapi/infra"
	"net/http"

	"github.com/gin-gonic/gin"
)

type RestController struct {
	aggregationSchemasService *application.SchemaAggregationService
}

func NewRestController(aggregationSchemasService *application.SchemaAggregationService) *RestController {
	return &RestController{
		aggregationSchemasService: aggregationSchemasService,
	}
}

func (rc *RestController) CreateSchema(c *gin.Context) {
	var schemaDTO infra.SchemaDTO
	err := c.ShouldBindJSON(&schemaDTO)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error reading schema: %v", err).Error()})
		return
	}
	id, err := rc.aggregationSchemasService.CreateAggregationSchema(schemaDTO)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("Error creating schema: %v", err).Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": id})
}

func (rc *RestController) GetSchemas(c *gin.Context) {
	schemas, err := rc.aggregationSchemasService.GetAggregationSchemas()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, schemas)
}
