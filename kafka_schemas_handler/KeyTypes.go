package kafka_schemas_handler

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"
)

type KeyTypes struct {
	SCHEMA         string
	DELETE_SUBJECT string
	allTypes       []string
}

func NewKeyTypes() *KeyTypes {
	const (
		schema        = "SCHEMA"
		deleteSubject = "DELETE_SUBJECT"
	)
	allTypes := []string{schema, deleteSubject}
	return &KeyTypes{SCHEMA: schema, DELETE_SUBJECT: deleteSubject, allTypes: allTypes}
}

func (kt *KeyTypes) GetKeytype(key []byte) (string, error) {
	var decoded map[string]any
	if err := json.Unmarshal(key, &decoded); err != nil {
		log.Printf("Error decoding key: %v", err)
		return "", err
	}
	keytypeRaw := decoded["keytype"]
	var keytype string
	switch value := keytypeRaw.(type) {
	case string:
		keytype = value
	default:
		err := fmt.Errorf("decoded keytype is not a string")
		return "", err
	}
	if !slices.Contains(kt.allTypes, keytype) {
		return "", fmt.Errorf("keytype %s not supported", keytype)
	}
	return keytype, nil
}
