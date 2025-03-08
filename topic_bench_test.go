package topic

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"testing"
	"time"
)

const testMessage = `
# Read system health check
path "sys/health"
{
capabilities = ["read", "sudo"]
}

# Create and manage ACL policies broadly across Vault

# List existing policies
path "sys/policies/acl"
{
capabilities = ["list"]
}

# Create and manage ACL policies
path "sys/policies/acl/*"
{
capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

# Enable and manage authentication methods broadly across Vault

# Manage auth methods broadly across Vault
path "auth/*"
{
capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

# Create, update, and delete auth methods
path "sys/auth/*"
{
capabilities = ["create", "update", "delete", "sudo"]
}

# List auth methods
path "sys/auth"
{
capabilities = ["read"]
}

# Enable and manage the key/value secrets engine at 'secret/' path

# List, create, update, and delete key/value secrets
path "secret/*"
{
capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

# Manage secrets engines
path "sys/mounts/*"
{
capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

# List existing secrets engines.
path "sys/mounts"
{
capabilities = ["read"]
}
`

func BenchmarkPublish(b *testing.B) {
	topic := NewTopic(context.Background(), 15*time.Minute, time.Minute, hclog.Info)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.Publish(testMessage)
	}
}

func BenchmarkSubscribe(b *testing.B) {
	topic := NewTopic(context.Background(), 15*time.Minute, time.Minute, hclog.Info)

	for i := 0; i < 1_000_000; i++ {
		topic.Publish(testMessage)
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
	topic := NewTopic(context.Background(), 30*time.Second, time.Second, hclog.Info)

	now := time.Now()
	for i := 0; i < 1_000_000; i++ {
		if i%2 == 0 {
			topic.messages = append(topic.messages, &Message{
				Offset:     i,
				Data:       testMessage,
				Expiration: now.Add(time.Minute),
			})

			continue
		}

		topic.messages = append(topic.messages, &Message{
			Offset:     i,
			Data:       testMessage,
			Expiration: now.Add(-time.Second),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic.cleanupWorker()
	}
}
