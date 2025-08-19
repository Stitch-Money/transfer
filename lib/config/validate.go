package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func ValidateMode(mode Mode) error {
	valid := []Mode{History, Replication}
	if !slices.Contains(valid, mode) {
		return fmt.Errorf("invalid mode supplied: %s", mode)
	}
	return nil
}

func ValidateOutputSource(source constants.DestinationKind) error {
	if !slices.Contains(constants.ValidDestinations, source) {
		return fmt.Errorf("invalid destination supplied: %s", source)
	}
	return nil
}

func ValidateQueueKind(queue constants.QueueKind) error {
	valid := []constants.QueueKind{constants.Kafka, constants.Reader}
	if !slices.Contains(valid, queue) {
		return fmt.Errorf("invalid queue kind supplied: %s", queue)
	}
	return nil
}

func ValidateTopicConfigs(topicConfigs []*kafkalib.TopicConfig) error {
	topicHashSet := make(map[string]int)
	for idx, topicConfig := range topicConfigs {
		hasher := sha256.New()
		hasher.Write([]byte(topicConfig.Database))
		hasher.Write([]byte(topicConfig.Schema))
		hasher.Write([]byte(topicConfig.TableName))
		hasher.Write([]byte(topicConfig.Topic))
		sha := hex.EncodeToString(hasher.Sum(nil))
		if _, ok := topicHashSet[sha]; !ok {
			topicHashSet[sha] = idx
		} else {
			return fmt.Errorf("topic config: %v conflicts with previous: %v, check database, schema, table and topic don't overlap",
				*(topicConfigs[idx]),
				*(topicConfigs[topicHashSet[sha]]),
			)
		}
	}
	return nil
}

func ValidateKafka(kafkaConfig *kafkalib.Kafka) error {
	return ValidateTopicConfigs(kafkaConfig.TopicConfigs)
}

func ValidateConfig(config Config) error {
	modeValidation := ValidateMode(config.Mode)
	if modeValidation != nil {
		return modeValidation
	}
	destinationSource := ValidateOutputSource(config.Output)
	if destinationSource != nil {
		return destinationSource
	}
	queueKind := ValidateQueueKind(config.Queue)
	if queueKind != nil {
		return queueKind
	}
	topicConfigs := ValidateTopicConfigs(config.Kafka.TopicConfigs)
	if topicConfigs != nil {
		return topicConfigs
	}

	return nil
}

func ValidateSettings(settings *Settings) error {
	return ValidateConfig(settings.Config)
}
