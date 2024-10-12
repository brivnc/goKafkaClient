package kafkaclient

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
)

type Admin struct {
	client *kadm.Client
}

func NewAdmin(brokers []string) *Admin {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Panicf("Unable to create new Client, %v", err)
	}
	admin := kadm.NewClient(client)
	return &Admin{client: admin}
}

func (a *Admin) TopicExists(topic string) bool {
	ctx := context.Background()
	topics, err := a.client.ListTopics(ctx, topic)
	if err != nil {
		log.Panicf("Topic does not exist: %v", err)
	}
	for _, metadata := range topics {
		if metadata.Topic == topic {
			fmt.Printf("Found topic: %s\n", topic)
			return true
		}
	}
	return false
}

func (a *Admin) CreateTopic(topic string) {
	ctx := context.Background()
	resp, err := a.client.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil {
		log.Panicf("Unable to create topic '%s' due to request error: %v", topic, err)
	}
	if resp.Err != nil {
		log.Panicf("Unable to create topic '%s' due to Kafka error: %v", topic, resp.Err)
	}
	fmt.Printf("Created topic '%s'\n", topic)
}

func (a *Admin) Close() {
	a.client.Close()
}
