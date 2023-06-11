package main

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli *redis.Client
}

type Message struct {
	Message   string `json:"message"`
	Sender    string `json:"sender"`
	Timestamp int64  `json:"timestamp"`
}

func (c *RedisClient) InitClient(ctx context.Context, address, password string) error {
	r := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       0,        // use default DB
	})

	err := r.Ping(ctx).Err()
	if err != nil {
		return err
	}
	c.cli = r
	return nil
}

func (c *RedisClient) SaveMsg(ctx context.Context, chat string, message *Message) error {
	text, err := json.Marshal(message)
	if err != nil {
		return err
	}

	member := &redis.Z{
		Score:  float64(message.Timestamp),
		Member: text,
	}

	_, err = c.cli.ZAdd(ctx, chat, *member).Result()
	if err != nil {
		return err
	}
	return nil

}

func (c *RedisClient) GetMessagesByChat(ctx context.Context, chat string, start, end int64, reverse bool) ([]*Message, error) {
	var (
		rawMsgs  []string
		messages []*Message
		err      error
	)

	if reverse {
		rawMsgs, err = c.cli.ZRevRange(ctx, chat, start, end).Result()
		if err != nil {
			return nil, err
		}
	} else {
		rawMsgs, err = c.cli.ZRange(ctx, chat, start, end).Result()
		if err != nil {
			return nil, err
		}
	}

	for _, msg := range rawMsgs {
		tmp := &Message{}
		err := json.Unmarshal([]byte(msg), tmp)
		if err != nil {
			return nil, err
		}
		messages = append(messages, tmp)
	}

	return messages, nil
}
