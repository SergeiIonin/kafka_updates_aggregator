package configs

import (
	"fmt"
	"html/template"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type ConfigReader struct {
}

func GetPath(base string, sub string) string {
	return fmt.Sprintf("%s/%s", base, sub)
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

func valuesFromYamlFile(valuesFile string) (map[string]interface{}, error) {
	data, err := openFile(valuesFile)
	if err != nil {
		return nil, errors.Wrap(err, "opening data file")
	}
	defer func(data *os.File) {
        err := data.Close()
        if err != nil {
            log.Printf("Error closing config: %v", err)
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

func parse(templateFile, valuesFile, outputFile string) error {
	tmpl, err := parseFile(templateFile)
	if err != nil {
		return errors.Wrap(err, "parsing template file")
	}
	values, err := valuesFromYamlFile(valuesFile)
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

func (cr *ConfigReader) ReadConfig(templateFile, valuesFile, outputFile string) error {
	return parse(templateFile, valuesFile, outputFile)
}
