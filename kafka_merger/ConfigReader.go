package kafka_merger

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"html/template"
	"io"
    "log"
    "os"
)

type ConfigReader struct {
}

func NewConfigReader() *ConfigReader {
	return &ConfigReader{}
}

var (
	parseFile           = template.ParseFiles
	openFile            = os.Open
	createFile          = os.Create
	ioReadAll           = io.ReadAll
	yamlUnmarshal       = yaml.Unmarshal
	executeTemplateFile = func(templateFile *template.Template, wr io.Writer, data any) error {
		return templateFile.Execute(wr, data)
	}
)

func valuesFromYamlFile(dataFile string) (map[string]interface{}, error) {
	data, err := openFile(dataFile)
	if err != nil {
		return nil, errors.Wrap(err, "opening data file")
	}
	defer func(data *os.File) {
        err := data.Close()
        if err != nil {
            log.Printf("Error closing config for kafka_merger: %v", err)
        }
    }(data)

	s, err := ioReadAll(data)
	if err != nil {
		return nil, errors.Wrap(err, "reading data file")
	}
	var values map[string]interface{}
	err = yamlUnmarshal(s, &values)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling yaml file")
	}
	return values, nil
}

func parse(templateFile, dataFile, outputFile string) error {
	tmpl, err := parseFile(templateFile)
	if err != nil {
		return errors.Wrap(err, "parsing template file")
	}
	values, err := valuesFromYamlFile(dataFile)
	if err != nil {
		return err
	}
	output, err := createFile(outputFile)
	if err != nil {
		return errors.Wrap(err, "creating output file")
	}
	defer output.Close()
	err = executeTemplateFile(tmpl, output, values)
	if err != nil {
		return errors.Wrap(err, "executing template file")
	}
	return nil
}

func (cr *ConfigReader) ReadConfig(configTemplate, valuesPath, outputPath string) error {
	return parse(configTemplate, valuesPath, outputPath)
}
