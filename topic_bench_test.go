package topic

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"testing"
	"time"
)

func BenchmarkPublish(b *testing.B) {
	topic := NewTopic(context.Background(), 20*time.Millisecond, 20*time.Millisecond, hclog.Off)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Publish("test message")
	}
}

func BenchmarkSubscribe(b *testing.B) {
	topic := NewTopic(context.Background(), 20*time.Millisecond, 20*time.Millisecond, hclog.Off)

	for i := 0; i < 100000; i++ {
		topic.Publish("test message")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sub := topic.Subscribe(ctx, "benchmark_sub")
		for pb.Next() {
			_, ok := <-sub
			if !ok {
				break
			}
		}
	})
}

func BenchmarkCleanupWorker(b *testing.B) {
	topic := NewTopic(context.Background(), 15*time.Second, time.Second, hclog.Off)

	now := time.Now()
	for i := 0; i < 100_000_000; i++ {
		topic.messages = append(topic.messages, &Message{
			Offset:     i,
			Data:       "old message",
			Timestamp:  now.Add(-time.Minute),
			Expiration: now.Add(-time.Second),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.cleanupWorker()
	}
}
