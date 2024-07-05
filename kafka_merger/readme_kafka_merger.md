To initialize kafka_merger:

```go
package kafka_merger

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

var (
	configTplPath string
	valuesPath    string
	configPath    string
	configFile    []byte
)

func init() {
	pwd, _ := os.Getwd()
	configTplPath = fmt.Sprintf("%s/kafka_merger/config/templates/merger_config_template.yaml", pwd)
	valuesPath = fmt.Sprintf("%s/kafka_merger/config/values.yaml", pwd)
	configPath = fmt.Sprintf("%s/kafka_merger/config/merger_config.yaml", pwd)

	configReader := NewConfigReader()

	err := configReader.ReadConfig(configTplPath, valuesPath, configPath)
	if err != nil {
		log.Fatalf("Error creating merger_config: %v", err)
	}
	configFile, err = os.ReadFile(configPath)
}

func main() {
	var kafkaMergerConfig KafkaMergerConfig
	err := yaml.Unmarshal(configFile, &kafkaMergerConfig)
	if err != nil {
		log.Fatalf("Error parsing kafkaMergerConfig file: %v", err)
	}
}
```
