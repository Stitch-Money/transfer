package config

import (
	"testing"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
)

func TestValidateTopicConfigs(t *testing.T) {
	// ensure duplication is picked up
	input := []*kafkalib.TopicConfig{}

	config := &kafkalib.TopicConfig{
		Database:  "database",
		Schema:    "schema",
		TableName: "table",
	}
	input = append(input, config)

	input = append(input, config)

	err := ValidateTopicConfigs(input)
	assert.NotEmpty(t, input)
	assert.Error(t, err)
}
