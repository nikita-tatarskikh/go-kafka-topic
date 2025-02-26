package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Message struct {
	Offset     int
	Data       any
	Timestamp  time.Time
	Expiration time.Time
}

type Topic struct {
	mu       sync.Mutex
	messages []Message
	ttl      time.Duration
	offset   int
	cond     *sync.Cond
	ctx      context.Context
	cancel   context.CancelFunc
	closed   bool
}

func NewTopic(ctx context.Context, ttl time.Duration) *Topic {
	ctx, cancel := context.WithCancel(ctx)
	t := &Topic{
		messages: make([]Message, 0),
		ttl:      ttl,
		offset:   0,
		ctx:      ctx,
		cancel:   cancel,
		closed:   false,
	}
	t.cond = sync.NewCond(&t.mu)
	go t.cleanupWorker()
	return t
}

func (t *Topic) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.cond.Broadcast() // Пробуждаем подписчиков, чтобы они дочитали сообщения
	t.cancel()
}

func (t *Topic) Publish(data any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("topic closed, cannot publish")
	}

	msg := Message{
		Offset:     t.offset,
		Data:       data,
		Timestamp:  time.Now(),
		Expiration: time.Now().Add(t.ttl),
	}
	t.messages = append(t.messages, msg)
	t.offset++
	t.cond.Broadcast()
	return nil
}

func (t *Topic) cleanupWorker() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.mu.Lock()
			now := time.Now()
			i := 0
			for i < len(t.messages) && t.messages[i].Expiration.Before(now) {
				i++
			}
			t.messages = t.messages[i:]
			t.mu.Unlock()
		}
	}
}

func (t *Topic) Subscribe(ctx context.Context, offset int) <-chan Message {
	ch := make(chan Message, 100)

	go func() {
		defer close(ch)

		for {
			t.mu.Lock()

			// Отправка сообщений подписчику
			for offset < len(t.messages) {
				if t.messages[offset].Expiration.After(time.Now()) {
					select {
					case <-ctx.Done(): // Завершаем подписку, если подписчик закрылся
						t.mu.Unlock()
						return
					case ch <- t.messages[offset]: // Отправляем сообщение
						offset++
					}
				} else {
					offset++
				}
			}

			// Если топик закрыт, но есть сообщения, дочитываем их
			if t.closed {
				t.mu.Unlock()
				// Дочитываем остаток без блокировок
				for offset < len(t.messages) {
					if t.messages[offset].Expiration.After(time.Now()) {
						ch <- t.messages[offset]
					}
					offset++
				}
				return // После дочитывания подписчик завершается
			}

			// Ожидание новых сообщений или завершения контекста
			select {
			case <-ctx.Done():
				t.mu.Unlock()
				return
			default:
				t.cond.Wait()
			}

			t.mu.Unlock()
		}
	}()
	return ch
}

func main() {
	rootCtx, cancel := context.WithCancel(context.Background())
	topic := NewTopic(rootCtx, 15*time.Minute)

	// Подписчик
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	go func() {
		sub := topic.Subscribe(subCtx, 0)
		for msg := range sub {
			fmt.Printf("Received message: %s (offset: %d)\n", msg.Data, msg.Offset)
		}
		fmt.Println("Subscriber finished reading all messages.")
	}()

	// Публикация сообщений
	topic.Publish([]byte("Message 1"))
	topic.Publish([]byte("Message 2"))

	// Даем время на обработку
	time.Sleep(1 * time.Second)

	// Завершаем топик
	fmt.Println("Closing topic...")
	cancel()
	topic.Close()

	// Даем подписчику время дочитать
	time.Sleep(2 * time.Second)

	fmt.Println("Shutdown complete.")
}
