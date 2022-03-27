package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/twmb/franz-go/pkg/kgo"
)

type OutputTwitterTagsMessage struct {
	Tags    []string
	Message string
	Author  string
}

type TwitterTopLevel struct {
}

// ProcessData - Process data being read from kafka
func ProcessTwitterTagsMessage(m *kgo.Record) (*OutputTwitterTagsMessage, error) {
	var tmsg twitter.Tweet

	err := json.Unmarshal(m.Value, &tmsg)
	if err != nil {
		return nil, err
	}

	var ht []string
	for _, h := range tmsg.Entities.Hashtags {
		ht = append(ht, h.Text)
	}

	// Adding Sleep to slow it down a bit..
	time.Sleep(400 * time.Millisecond)

	return &OutputTwitterTagsMessage{
		Tags:    ht,
		Message: tmsg.Text,
		Author:  tmsg.User.ScreenName,
	}, nil
}

// WriteToTwitterTags - Process pipe and sink to kafka
func WriteToTwitterTags(ctx context.Context, client *kgo.Client, topic string, m interface{}) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Timestamp: time.Now(),
		Topic:     topic,
		Value:     payload,
	}

	client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}
	})
	return nil
}
