package domain

// fixme fields should be unexpported and we can only access them
type Schema struct {
	Subject string   `json:"subject"`
	Version int      `json:"version"`
	ID      int      `json:"id"`
	Fields  []string `json:"fields"`
	Schema  string   `json:"schema"` // a raw json schema
}
