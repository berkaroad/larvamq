package v1

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/berkaroad/uuid"
)

var version = [2]byte{'V', 1}

type MessageSendRequest struct {
	Version       [2]byte
	RouteKey      uint16
	UnixTimestamp int64 // 精确到毫秒
	MessageID     uuid.UUID
	Size          int
	MessageBody   []byte
}

func NewMessageSendRequest(routeKey uint16, messageID uuid.UUID, body []byte) *MessageSendRequest {
	return &MessageSendRequest{
		Version:       version,
		RouteKey:      routeKey,
		UnixTimestamp: time.Now().UnixNano() / 1000000,
		MessageID:     messageID,
		Size:          len(body),
		MessageBody:   body}
}

func LoadMessageSendRequest(buffer []byte) (*MessageSendRequest, error) {
	if len(buffer) < 32 {
		return nil, errors.New("at least 32 bytes")
	}
	if buffer[0] != version[0] || buffer[1] != version[1] {
		fmt.Println("not V1")
		return nil, errors.New("not V1 version")
	}
	req := &MessageSendRequest{}
	req.Version = version
	req.RouteKey = uint16(buffer[2])<<8 + uint16(buffer[3])
	req.UnixTimestamp = int64(buffer[4])<<56 + int64(buffer[5])<<48 + int64(buffer[6])<<40 + int64(buffer[7])<<32 + int64(buffer[8])<<24 + int64(buffer[9])<<16 + int64(buffer[10])<<8 + int64(buffer[11])
	req.MessageID = uuid.UUID([16]byte{buffer[12], buffer[13], buffer[14], buffer[15], buffer[16], buffer[17], buffer[18], buffer[19], buffer[20], buffer[21], buffer[22], buffer[23], buffer[24], buffer[25], buffer[26], buffer[27]})
	req.Size = int(buffer[28])<<24 + int(buffer[29])<<16 + int(buffer[30])<<8 + int(buffer[31])

	if len(buffer)-32 < req.Size {
		return nil, errors.New("at least " + strconv.Itoa(32+req.Size) + " bytes")
	}
	if req.Size > 0 {
		req.MessageBody = buffer[32 : 32+req.Size]
	}
	return req, nil
}

func (r *MessageSendRequest) ToBytes() []byte {
	buffer := make([]byte, 32)
	buffer[0] = r.Version[0]
	buffer[1] = r.Version[1]

	buffer[2] = byte(r.RouteKey >> 8)
	buffer[3] = byte(r.RouteKey)

	buffer[4] = byte(r.UnixTimestamp >> 56)
	buffer[5] = byte(r.UnixTimestamp >> 48)
	buffer[6] = byte(r.UnixTimestamp >> 40)
	buffer[7] = byte(r.UnixTimestamp >> 32)
	buffer[8] = byte(r.UnixTimestamp >> 24)
	buffer[9] = byte(r.UnixTimestamp >> 16)
	buffer[10] = byte(r.UnixTimestamp >> 8)
	buffer[11] = byte(r.UnixTimestamp)

	buffer[12] = r.MessageID[0]
	buffer[13] = r.MessageID[1]
	buffer[14] = r.MessageID[2]
	buffer[15] = r.MessageID[3]
	buffer[16] = r.MessageID[4]
	buffer[17] = r.MessageID[5]
	buffer[18] = r.MessageID[6]
	buffer[19] = r.MessageID[7]
	buffer[20] = r.MessageID[8]
	buffer[21] = r.MessageID[9]
	buffer[22] = r.MessageID[10]
	buffer[23] = r.MessageID[11]
	buffer[24] = r.MessageID[12]
	buffer[25] = r.MessageID[13]
	buffer[26] = r.MessageID[14]
	buffer[27] = r.MessageID[15]

	buffer[28] = byte(r.Size >> 24)
	buffer[29] = byte(r.Size >> 16)
	buffer[30] = byte(r.Size >> 8)
	buffer[31] = byte(r.Size)

	if r.Size > 0 {
		buffer = append(buffer, r.MessageBody...)
	}
	return buffer
}

type MessageSendResponse struct {
	Version   [2]byte
	MessageId uuid.UUID
	IsSucceed bool
	Size      int
	ErrMsg    string
}

func NewMessageSendResponse(messageId uuid.UUID, isSucceed bool, errMsg string) *MessageSendResponse {
	return &MessageSendResponse{MessageId: messageId, IsSucceed: isSucceed, Size: len([]byte(errMsg)), ErrMsg: errMsg}
}

func LoadMessageSendResponse(buffer []byte) (*MessageSendResponse, error) {
	if len(buffer) < 23 {
		return nil, errors.New("at least 23 bytes")
	}
	if buffer[0] != version[0] || buffer[1] != version[1] {
		fmt.Println("not V1")
		return nil, errors.New("not V1 version")
	}
	res := &MessageSendResponse{}
	res.Version = version
	res.MessageId = uuid.UUID([16]byte{buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13], buffer[14], buffer[15], buffer[16], buffer[17]})
	if buffer[18] == 1 {
		res.IsSucceed = true
	}
	if res.IsSucceed {
		res.Size = 1
	} else {
		res.Size = int(buffer[28])<<24 + int(buffer[29])<<16 + int(buffer[30])<<8 + int(buffer[31])
		if len(buffer)-23 < res.Size {
			return nil, errors.New("at least " + strconv.Itoa(23+res.Size) + " bytes")
		}
		if res.Size <= 0 {
			res.Size = 1
		} else {
			res.ErrMsg = string(buffer[23 : 23+res.Size])
		}
	}
	return res, nil
}

func (r *MessageSendResponse) ToBytes() []byte {
	buffer := make([]byte, 23)
	buffer[0] = r.Version[0]
	buffer[1] = r.Version[1]

	buffer[2] = r.MessageId[0]
	buffer[3] = r.MessageId[1]
	buffer[4] = r.MessageId[2]
	buffer[5] = r.MessageId[3]
	buffer[6] = r.MessageId[4]
	buffer[7] = r.MessageId[5]
	buffer[8] = r.MessageId[6]
	buffer[9] = r.MessageId[7]
	buffer[10] = r.MessageId[8]
	buffer[11] = r.MessageId[9]
	buffer[12] = r.MessageId[10]
	buffer[13] = r.MessageId[11]
	buffer[14] = r.MessageId[12]
	buffer[15] = r.MessageId[13]
	buffer[16] = r.MessageId[14]
	buffer[17] = r.MessageId[15]

	if r.IsSucceed {
		buffer[18] = 1

		buffer[22] = 1

		buffer = append(buffer, 0)
	} else {
		buffer[19] = byte(r.Size >> 24)
		buffer[20] = byte(r.Size >> 16)
		buffer[21] = byte(r.Size >> 8)
		buffer[22] = byte(r.Size)
		if r.Size > 0 {
			buffer = append(buffer, []byte(r.ErrMsg)...)
		}
	}
	return buffer
}
