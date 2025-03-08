package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"slices"
	"sync"
	"time"
)

type Message struct {
	Offset     int       `json:"offset"`
	Data       any       `json:"data"`
	Expiration time.Time `json:"expiration"`
}

func (m Message) JSON() string {
	j, _ := json.Marshal(m)
	return string(j)
}

type Topic struct {
	lock            sync.RWMutex
	messages        []*Message
	ttl             time.Duration
	cleanupInterval time.Duration
	offset          int
	minOffset       int
	sync            *sync.Cond
	ctx             context.Context
	cancel          context.CancelFunc
	logger          hclog.Logger
}

func NewTopic(ctx context.Context, ttl, cleanupInterval time.Duration, logLevel hclog.Level) *Topic {
	ctx, cancel := context.WithCancel(ctx)
	t := &Topic{
		messages:        make([]*Message, 0),
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		offset:          0,
		minOffset:       0,
		ctx:             ctx,
		cancel:          cancel,
		logger: hclog.New(&hclog.LoggerOptions{
			JSONFormat:         true,
			JSONEscapeDisabled: true,
		}),
	}

	t.logger.SetLevel(logLevel)

	t.sync = sync.NewCond(&t.lock)
	go t.cleanupWorker()
	return t
}

func (t *Topic) Publish(data any) {
	t.lock.Lock()
	defer t.lock.Unlock()

	select {
	case <-t.ctx.Done():
		t.logger.Debug("topic closed, skipping")
		return
	default:
	}

	msg := &Message{
		Offset:     t.offset,
		Data:       data,
		Expiration: time.Now().Add(t.ttl),
	}

	t.messages = append(t.messages, msg)
	t.offset++

	t.sync.Broadcast()
}

func (t *Topic) cleanupWorker() {
	ticker := time.NewTicker(t.cleanupInterval)
	defer ticker.Stop()

	logger := t.logger.Named("cleanup")
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.lock.Lock()
			logger.Debug("starting cleanup")
			now := time.Now()
			expiredMessages := 0

			t.messages = slices.DeleteFunc(t.messages, func(message *Message) bool {
				if message.Expiration.Before(now) {
					expiredMessages++
					return true
				}

				return false
			})

			logger.Debug("messages expired", "count", expiredMessages)

			if expiredMessages == 0 {
				t.lock.Unlock()
				return
			}

			if len(t.messages) == 0 {
				t.minOffset = t.offset
			} else {
				t.minOffset = t.messages[0].Offset
			}

			logger.Debug("new minimal offset", "offset", t.minOffset)
			logger.Debug("cleanup complete")
			t.lock.Unlock()
		}
	}
}

func (t *Topic) Subscribe(ctx context.Context, consumerName string) <-chan *Message {
	ch := make(chan *Message, 1_000_000)

	consumerOffset := 0
	logger := t.logger.Named(consumerName)

	go func() {
		defer close(ch)

		for {
			t.lock.Lock()
			if len(t.messages) == 0 {
				select {
				case <-ctx.Done():
					t.lock.Unlock()
					return
				case <-t.ctx.Done():
					t.lock.Unlock()
					return
				default:
					logger.Debug("no messages, waiting for messages")
					t.sync.Wait()
				}
			}

			if consumerOffset <= t.minOffset {
				logger.Debug(fmt.Sprintf("consumer offset adjusted: %d", consumerOffset))
				consumerOffset = t.minOffset
			}

			for consumerOffset < t.offset {
				select {
				case <-t.ctx.Done():
					t.lock.Unlock()
					for i := consumerOffset; i < t.offset; i++ {
						ch <- t.messages[i-t.minOffset]
					}

					return
				case <-ctx.Done():
					t.lock.Unlock()
					return
				case ch <- t.messages[consumerOffset-t.minOffset]:
					logger.Debug("got message", "message", t.messages[consumerOffset-t.minOffset].JSON())
					consumerOffset++
				}
			}

			select {
			case <-ctx.Done():
				t.lock.Unlock()
				return
			case <-t.ctx.Done():
				t.lock.Unlock()
				return
			default:
				logger.Debug("lag processed, waiting for messages")
				t.sync.Wait()
			}

			t.lock.Unlock()
		}
	}()

	return ch
}

func (t *Topic) Cancel() {
	t.lock.Lock()
	t.cancel()
	t.logger.Debug("topic closed")
	t.sync.Broadcast()
	t.lock.Unlock()
}
