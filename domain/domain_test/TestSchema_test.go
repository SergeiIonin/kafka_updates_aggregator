package domain_test

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"kafka_updates_aggregator/domain"
	"testing"
)

func TestSchema_test(t *testing.T) {
	sc0 := domain.CreateSchema("user_balance_updates", 1, 1,
		[]domain.Field{{"user_id", "string"}, {"balance", "int"}, {"deposit", "int"}, {"withdrawal", "int"}},
		`{
						"type": "record",
						"name": "user_balance_updates",
						"fields": [
								{"name": "user_id", "type": "string"},
								{"name": "balance", "type": "int"},
								{"name": "deposit", "type": "int"},
								{"name": "withdrawal", "type": "int"}
							]
					}`)
	t.Run("encode and decode schema", func(t *testing.T) {
		encoded, err := json.Marshal(sc0)
		if err != nil {
			t.Fatalf("Error marshalling schema: %v", err)
		}
		t.Logf("Encoded schema: %v", string(encoded))

		var decodedSchema domain.Schema
		err = json.Unmarshal(encoded, &decodedSchema)
		if err != nil {
			t.Fatalf("Error unmarshalling schema: %v", err)
		}
		t.Logf("Decoded schema: %v", decodedSchema)
		assert.Equal(t, sc0.Subject(), decodedSchema.Subject())
		assert.Equal(t, sc0.Version(), decodedSchema.Version())
		assert.Equal(t, sc0.ID(), decodedSchema.ID())
	})
}
