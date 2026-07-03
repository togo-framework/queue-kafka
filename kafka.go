// Package queuekafka is a togo queue backend backed by Apache Kafka. Implements
// togo.Queue (topic per job name); takes over k.Queue when selected with
// `togo provider:use queue kafka`.
//
//   KAFKA_BROKERS  comma-separated brokers (default localhost:9092)
//   KAFKA_GROUP    consumer group id       (default "togo")
package queuekafka

import (
	"context"
	"encoding/json"
	"strings"

	kafka "github.com/segmentio/kafka-go"
	"github.com/togo-framework/providers"
	"github.com/togo-framework/togo"
)

var _ togo.Queue = (*kafkaQueue)(nil)

func init() {
	togo.RegisterProviderFunc("queue-kafka", togo.PriorityLate+1, func(k *togo.Kernel) error {
		if providers.Active(k, providers.CapQueue, "memory") != "kafka" {
			return nil
		}
		k.Queue = newKafka(k)
		if k.Log != nil {
			k.Log.Info("plugin active", "plugin", "queue-kafka")
		}
		return nil
	})
}

type kafkaQueue struct {
	brokers []string
	group   string
}

func newKafka(k *togo.Kernel) *kafkaQueue {
	brokers := splitBrokers(providers.Value(k, providers.CapQueue, "kafka", "brokers", "localhost:9092", false))
	return &kafkaQueue{brokers: brokers, group: providers.Value(k, providers.CapQueue, "kafka", "group", "togo", false)}
}

func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

func (q *kafkaQueue) Handle(name string, h togo.QueueHandler) {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: q.brokers, Topic: name, GroupID: q.group})
	go func() {
		defer r.Close()
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				return
			}
			var payload any
			_ = json.Unmarshal(m.Value, &payload)
			_ = h(context.Background(), payload)
		}
	}()
}

func (q *kafkaQueue) Dispatch(ctx context.Context, name string, payload any) error {
	w := &kafka.Writer{Addr: kafka.TCP(q.brokers...), Topic: name, Balancer: &kafka.LeastBytes{}, AllowAutoTopicCreation: true}
	defer w.Close()
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{Value: b})
}
