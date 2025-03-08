package topic

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestTopic_Capacity(t *testing.T) {
	t.Parallel()
	messageTTL := 20 * time.Millisecond
	cleanupInterval := 20 * time.Millisecond

	topic := NewTopic(context.Background(), messageTTL, cleanupInterval, hclog.Off)

	wg := sync.WaitGroup{}
	wg.Add(3)

	testData := generateTestData()
	go func() {
		defer wg.Done()
		for _, message := range testData {
			topic.Publish(message)
		}
		topic.Cancel()
	}()

	var consumer1res, consumer2res []int

	go func() {
		defer wg.Done()
		for event := range topic.Subscribe(context.Background(), "consumer-1") {
			consumer1res = append(consumer1res, event.Data.(int))
		}
	}()

	go func() {
		defer wg.Done()
		for event := range topic.Subscribe(context.Background(), "consumer-2") {
			consumer2res = append(consumer2res, event.Data.(int))
		}
	}()

	wg.Wait()

	assert.Equal(t, testData, consumer1res, "Consumer 1 should receive all messages")
	assert.Equal(t, testData, consumer2res, "Consumer 2 should receive all messages")
}

func TestTopic_CapacityV2(t *testing.T) {
	t.Parallel()
	messageTTL := 500 * time.Millisecond
	cleanupInterval := 100 * time.Millisecond

	topic := NewTopic(context.Background(), messageTTL, cleanupInterval, hclog.Off)

	test := sync.WaitGroup{}
	test.Add(3)

	testData := generateTestData()

	var consumer1res, consumer2res []int
	startConsumers := sync.WaitGroup{}
	startConsumers.Add(2)

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(context.Background(), "consumer-1") {
			consumer1res = append(consumer1res, event.Data.(int))
		}
	}()

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(context.Background(), "consumer-2") {
			consumer2res = append(consumer2res, event.Data.(int))
		}
	}()

	startConsumers.Wait()

	go func() {
		defer test.Done()
		for _, message := range testData {
			topic.Publish(message)
		}
		time.Sleep(100 * time.Millisecond)
		topic.Cancel()
	}()

	test.Wait()

	assert.Equal(t, testData, consumer1res, "Consumer 1 should receive all messages")
	assert.Equal(t, testData, consumer2res, "Consumer 2 should receive all messages")
}

func TestTopic_CancelTopicNoMessages(t *testing.T) {
	t.Parallel()
	messageTTL := 500 * time.Millisecond
	cleanupInterval := 100 * time.Millisecond

	topic := NewTopic(context.Background(), messageTTL, cleanupInterval, hclog.Off)

	test := sync.WaitGroup{}
	test.Add(3)

	var consumer1res, consumer2res []int
	startConsumers := sync.WaitGroup{}
	startConsumers.Add(2)

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(context.Background(), "consumer-1") {
			consumer1res = append(consumer1res, event.Data.(int))
		}
	}()

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(context.Background(), "consumer-2") {
			consumer2res = append(consumer2res, event.Data.(int))
		}
	}()

	startConsumers.Wait()

	go func() {
		defer test.Done()
		topic.Cancel()
	}()

	test.Wait()

	assert.Equal(t, []int(nil), consumer1res, "Consumer 1 should have no data")
	assert.Equal(t, []int(nil), consumer2res, "Consumer 2 should have no data")
}

func TestTopic_CancelConsumersNoMessages(t *testing.T) {
	t.Parallel()
	messageTTL := 500 * time.Millisecond
	cleanupInterval := 100 * time.Millisecond

	topic := NewTopic(context.Background(), messageTTL, cleanupInterval, hclog.Off)

	test := sync.WaitGroup{}
	test.Add(3)

	var consumer1res, consumer2res []int
	startConsumers := sync.WaitGroup{}
	startConsumers.Add(2)

	consumer1ctx, consumer1cancel := context.WithCancel(context.Background())
	consumer2ctx, consumer2cancel := context.WithCancel(context.Background())

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(consumer1ctx, "consumer-1") {
			consumer1res = append(consumer1res, event.Data.(int))
		}
	}()

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(consumer2ctx, "consumer-2") {
			consumer2res = append(consumer2res, event.Data.(int))
		}
	}()

	startConsumers.Wait()

	go func() {
		defer test.Done()
		consumer1cancel()
		consumer2cancel()
	}()

	test.Wait()

	assert.Equal(t, []int(nil), consumer1res, "Consumer 1 should have no data")
	assert.Equal(t, []int(nil), consumer2res, "Consumer 2 should have no data")
}

func TestTopic_CancelConsumersWhileProcessing(t *testing.T) {
	t.Parallel()
	messageTTL := 500 * time.Millisecond
	cleanupInterval := 100 * time.Millisecond

	testData := generateTestData()

	topic := NewTopic(context.Background(), messageTTL, cleanupInterval, hclog.Off)

	test := sync.WaitGroup{}
	test.Add(3)

	var consumer1res, consumer2res []int
	startConsumers := sync.WaitGroup{}
	startConsumers.Add(2)

	consumer1ctx, consumer1cancel := context.WithCancel(context.Background())
	consumer2ctx, consumer2cancel := context.WithCancel(context.Background())

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(consumer1ctx, "consumer-1") {
			consumer1res = append(consumer1res, event.Data.(int))
		}
	}()

	go func() {
		defer test.Done()
		startConsumers.Done()
		for event := range topic.Subscribe(consumer2ctx, "consumer-2") {
			consumer2res = append(consumer2res, event.Data.(int))
		}
	}()

	startConsumers.Wait()

	go func() {
		defer test.Done()
		go func() {
			for _, message := range testData {
				topic.Publish(message)
			}
		}()

		time.AfterFunc(200*time.Millisecond, func() {
			consumer1cancel()
			consumer2cancel()
		})
	}()

	test.Wait()

	assert.NotNil(t, consumer1res, "Consumer 1 should have some data")
	assert.NotNil(t, consumer2res, "Consumer 2 should have some data")
}

func generateTestData() []int {
	const maxCount = 1000000
	testData := make([]int, maxCount)
	for i := 0; i < maxCount; i++ {
		testData[i] = i
	}
	return testData
}
