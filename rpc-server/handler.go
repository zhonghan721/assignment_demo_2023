package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	err := validateSendReq(req)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().Unix()

	message := &Message{
		Message:   req.Message.GetText(),
		Sender:    req.Message.GetSender(),
		Timestamp: timestamp,
	}

	chatId, err := processChatId(req.Message.GetChat())
	if err != nil {
		return nil, err
	}

	err = rdb.SaveMsg(ctx, chatId, message)
	if err != nil {
		return nil, err
	}

	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	chatId, err := processChatId(req.GetChat())
	if err != nil {
		return nil, err
	}

	start := req.GetCursor()
	end := start + int64(req.GetLimit())

	messages, err := rdb.GetMessagesByChat(ctx, chatId, start, end, req.GetReverse())
	if err != nil {
		return nil, err
	}

	respMessages := make([]*rpc.Message, 0)
	var counter int32 = 0
	var nextCursor int64 = 0
	hasMore := false
	for _, msg := range messages {
		if counter+1 > req.GetLimit() {
			hasMore = true
			nextCursor = end
			break
		}
		tmp := &rpc.Message{
			Chat:     req.GetChat(),
			Text:     msg.Message,
			Sender:   msg.Sender,
			SendTime: msg.Timestamp,
		}
		respMessages = append(respMessages, tmp)
		counter += 1
	}

	resp := rpc.NewPullResponse()
	resp.Messages = respMessages
	resp.Code = 0
	resp.Msg = "success"
	resp.HasMore = &hasMore
	resp.NextCursor = &nextCursor

	return resp, nil
}

func areYouLucky() (int32, string) {
	if rand.Int31n(2) == 1 {
		return 0, "success"
	} else {
		return 500, "oops"
	}
}

func processChatId(chat string) (string, error) {
	var chatId string

	data := strings.Split(strings.ToLower(chat), ":")
	if len(data) != 2 {
		err := fmt.Errorf("invalid Chat Id")
		return "", err
	}

	sender1 := data[0]
	sender2 := data[1]

	if strings.Compare(sender1, sender2) == 1 {
		chatId = sender2 + ":" + sender1
	} else {
		chatId = sender1 + ":" + sender2
	}

	return chatId, nil
}

func validateSendReq(req *rpc.SendRequest) error {
	data := strings.Split(req.Message.Chat, ":")
	if len(data) != 2 {
		err := fmt.Errorf("invalid Chat Id")
		return err
	}

	sender1 := data[0]
	sender2 := data[1]

	if req.Message.GetSender() != sender1 && req.Message.GetSender() != sender2 {
		err := fmt.Errorf("sender does not belong to the chat room")
		return err
	}

	return nil
}
