package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
)

var ErrTombstoneDetected = errors.New("tombstone detected")

var topicToConsumer *TopicToConsumer

func NewTopicToConsumer() *TopicToConsumer {
	return &TopicToConsumer{
		topicToConsumer: make(map[string]kafkalib.Consumer),
	}
}

type TopicToConsumer struct {
	topicToConsumer map[string]kafkalib.Consumer
	sync.RWMutex
}

func (t *TopicToConsumer) Add(topic string, consumer kafkalib.Consumer) {
	t.Lock()
	defer t.Unlock()
	t.topicToConsumer[topic] = consumer
}

func (t *TopicToConsumer) Get(topic string) kafkalib.Consumer {
	t.RLock()
	defer t.RUnlock()
	return t.topicToConsumer[topic]
}

func ConfigureTcFmtMap(topicConfigs []*kafkalib.TopicConfig, outTcFmtMap *TcFmtMap) {
	for _, topicConfig := range topicConfigs {
		empty := stringutil.Empty(topicConfig.Database, topicConfig.Schema, topicConfig.Topic, topicConfig.CDCFormat)
		if empty {
			slog.Warn("database, schema, topic or cdc format is empty, skipping")
			continue
		}

		outTcFmtMap.Add(topicConfig.Topic, TopicConfigFormatter{
			tc:     *topicConfig,
			Format: format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic),
		})
	}
}

func CreateConsumer(cfg config.Config, dialer *kafka.Dialer, topic string) *kafka.Reader {
	kafkaCfg := kafka.ReaderConfig{
		GroupID: cfg.Kafka.GroupID,
		Dialer:  dialer,
		Topic:   topic,
		Brokers: cfg.Kafka.BootstrapServers(true),
	}

	return kafka.NewReader(kafkaCfg)
}

func CloseConsumer(reader *kafka.Reader) {
	err := reader.Close()
	if err != nil {
		slog.Error("failed to close consumer", "topic", reader.Config().Topic, "cause", err)
	} else {
		slog.Info("Closing Kafka Consumer.", "topic", reader.Config().Topic)
	}
}

func FetchArtieMessage(ctx context.Context, reader *kafka.Reader) (artie.Message, error) {

	kafkaMsg, err := reader.FetchMessage(ctx)
	if err != nil {
		slog.With(artie.KafkaMsgLogFields(kafkaMsg)...).Warn("Failed to read kafka message", slog.Any("err", err))
		return artie.Message{}, err
	} else if len(kafkaMsg.Value) == 0 {
		slog.Debug("Found a tombstone message", artie.KafkaMsgLogFields(kafkaMsg)...)
		return artie.Message{}, ErrTombstoneDetected
	} else {
		return artie.NewMessage(&kafkaMsg, kafkaMsg.Topic), nil
	}
}

func ProcessArtieMessage(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, msg artie.Message, groupId string, tcFmtMap *TcFmtMap) (string, error) {
	args := processArgs{
		Msg:                    msg,
		GroupID:                groupId,
		TopicToConfigFormatMap: tcFmtMap,
	}

	return args.process(ctx, cfg, inMemDB, dest, metricsClient)
}

func StartConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) {
	var kafkaConn kafkalib.Connection

	if cfg.Kafka.SaslMechanism == "PLAIN" {
		kafkaConn = kafkalib.NewSaslPlainConnection(cfg.Kafka.Username, cfg.Kafka.Password)
	} else {
		kafkaConn = kafkalib.NewConnection(cfg.Kafka.EnableAWSMSKIAM, cfg.Kafka.DisableTLS, cfg.Kafka.Username, cfg.Kafka.Password, kafkalib.DefaultTimeout)
	}

	slog.Info("Starting Kafka consumer...",
		slog.Any("config", cfg.Kafka),
		slog.Any("authMechanism", kafkaConn.Mechanism()),
	)

	dialer, err := kafkaConn.Dialer(ctx)
	if err != nil {
		logger.Panic("Failed to create Kafka dialer", slog.Any("err", err))
	}

	tcFmtMap := NewTcFmtMap()
	topicToConsumer = NewTopicToConsumer()
	ConfigureTcFmtMap(cfg.Kafka.TopicConfigs, tcFmtMap)

	var wg sync.WaitGroup
	for num, topic := range tcFmtMap.Topics() {
		// It is recommended to not try to establish a connection all at the same time, which may overwhelm the Kafka cluster.
		time.Sleep(jitter.Jitter(100, 3000, num))
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			kafkaConsumer := CreateConsumer(cfg, dialer, topic)
			defer CloseConsumer(kafkaConsumer)

			topicToConsumer.Add(topic, kafkaConsumer)
			for {
				group := kafkaConsumer.Config().GroupID
				topic := kafkaConsumer.Config().Topic
				select {
				case <-ctx.Done():
					// Unfortunately I can't see the consumer group generation and partition allocation.
					// This is managed by the Reader, via a ConsumerGroup which can't be accessed for information.
					slog.Info("Context Closed, no longer consuming.", slog.String("topic", topic), slog.String("group", group))
					return
				default:
					msg, err := FetchArtieMessage(ctx, kafkaConsumer)
					if err != nil {
						switch {
						case errors.Is(err, io.EOF):
							slog.Info("Consumer has been closed, exit consumption loop", "topic", topic)
							return
						case errors.Is(err, ErrTombstoneDetected):
							slog.Debug("Skipping tombstone message")
							continue
						default:
							time.Sleep(500 * time.Millisecond)
							continue
						}
					}

					tableName, processErr := ProcessArtieMessage(ctx, cfg, inMemDB, dest, metricsClient, msg, group, tcFmtMap)
					if processErr != nil {
						logger.Fatal("Failed to process message", slog.Any("err", processErr), slog.String("topic", topic))
					}

					msg.EmitIngestionLag(metricsClient, cfg.Mode, group, tableName)
					msg.EmitRowLag(metricsClient, cfg.Mode, group, tableName)
				}
			}
		}(topic)
	}

	wg.Wait()
}
