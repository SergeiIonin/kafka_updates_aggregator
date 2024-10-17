package keytypes

import (
	"encoding/json"
	"fmt"
	"log"
)

const (
	SCHEMA        = "SCHEMA"
	DELETE_SUBJECT = "DELETE_SUBJECT"
)

func GetKeytype(key []byte) (string, error) {
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
	if isValidKeytype(keytype) {
		return "", fmt.Errorf("keytype %s is not supported", keytype)
	}
	return keytype, nil
}

func isValidKeytype(keytype string) bool {
	if keytype == SCHEMA || keytype == DELETE_SUBJECT {
		return true
	}
	return false
}
