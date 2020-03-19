package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	pb "github.com/brocaar/chirpstack-api/go/v3/as/integration"
	"github.com/brocaar/chirpstack-application-server/internal/integration"
	"github.com/brocaar/chirpstack-application-server/internal/integration/marshaler"
	"github.com/brocaar/chirpstack-application-server/internal/logging"
	"github.com/brocaar/lorawan"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Config contains the configuration for the Kafka integration.
type Config struct {
	Server string `json:"server"`
}

// Integration implements a Kafka integration.
type Integration struct {
	marshaler marshaler.Type
	config    Config
}

// New creates a new Kafka integration.
func New(m marshaler.Type, conf Config) (*Integration, error) {
	return &Integration{
		marshaler: m,
		config: conf,
	}, nil
}

func (i *Integration) send(url string, msg proto.Message) error {
	b, err := marshaler.Marshal(i.marshaler, msg)
	if err != nil {
		return errors.Wrap(err, "marshal json error")
	}
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{i.config.Server}, kafkaConfig)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	kafkaMsg := &sarama.ProducerMessage{
		Topic:     "test",
		Partition: int32(-1),
		Key:       sarama.StringEncoder("key"),
	}
	kafkaMsg.Value = sarama.ByteEncoder(b)
	paritition, offset, err := producer.SendMessage(kafkaMsg)
	if err != nil {
		fmt.Println("Send Message Fail")
	}
	fmt.Printf("Partion = %d, offset = %d\n", paritition, offset)
	return nil
}

func (i *Integration) sendEvent(ctx context.Context, event, url string, devEUI lorawan.EUI64, msg proto.Message) {
	log.WithFields(log.Fields{
		"url":     url,
		"dev_eui": devEUI,
		"ctx_id":  ctx.Value(logging.ContextIDKey),
		"event":   event,
	}).Info("integration/kafka: publishing event")
	if err := i.send(url, msg); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"url":     url,
			"dev_eui": devEUI,
			"ctx_id":  ctx.Value(logging.ContextIDKey),
			"event":   event,
		}).Error("integration/kafka: publish event error")
	}
}

// SendDataUp sends a data-up payload.
func (i *Integration) SendDataUp(ctx context.Context, vars map[string]string, pl pb.UplinkEvent) error {
	var devEUI lorawan.EUI64
	copy(devEUI[:], pl.DevEui)

	for _, url := range getURLs(i.config.Server) {
		i.sendEvent(ctx, "up", url, devEUI, &pl)
	}

	return nil
}

func getURLs(str string) []string {
	urls := strings.Split(str, ",")
	var out []string

	for _, url := range urls {
		if url := strings.TrimSpace(url); url != "" {
			out = append(out, url)
		}
	}
	return out
}

// Close closes the handler.
func (i *Integration) Close() error {
	return nil
}

// DataDownChan returns nil.
func (i *Integration) DataDownChan() chan integration.DataDownPayload {
	return nil
}

// SendJoinNotification returns nil.
func (i *Integration) SendJoinNotification(ctx context.Context, vars map[string]string, pl pb.JoinEvent) error {
	return nil
}

// SendACKNotification returns nil.
func (i *Integration) SendACKNotification(ctx context.Context, vars map[string]string, pl pb.AckEvent) error {
	return nil
}

// SendErrorNotification returns nil.
func (i *Integration) SendErrorNotification(ctx context.Context, vars map[string]string, pl pb.ErrorEvent) error {
	return nil
}

// SendLocationNotification sends a location notification.
func (i *Integration) SendLocationNotification(ctx context.Context, vars map[string]string, pl pb.LocationEvent) error {
	return nil
}

// SendStatusNotification sends a status notification.
func (i *Integration) SendStatusNotification(ctx context.Context, vars map[string]string, pl pb.StatusEvent) error {
	return nil
}

// SendTxAckNotification sends a tx ack notification.
func (i *Integration) SendTxAckNotification(ctx context.Context, vars map[string]string, pl pb.TxAckEvent) error {
	return nil
}
