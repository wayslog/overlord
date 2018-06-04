package redis

import (
	"errors"

	"strings"

	"strconv"

	"github.com/felixhao/overlord/proto"
)

var (
	crlfBytes = []byte{'\r', '\n'}
	lfByte    = byte('\n')
)

// errors
var (
	ErrProxyFail        = errors.New("fail to send proxy")
	ErrRequestBadFormat = errors.New("redis must be a RESP array")
)

// respType is the type of redis resp
type respType = byte

// resp type define
const (
	respString = '+'
	respError  = '-'
	respInt    = ':'
	respBulk   = '$'
	respArray  = '*'
)

// resp is a redis resp protocol item.
type resp struct {
	rtype respType
	data  []byte
	array []*resp
}

func newRespBalk(data []byte) *resp {
	return &resp{
		rtype: respBulk,
		data:  data,
		array: nil,
	}
}

func newRespPlain(rtype respType, data []byte) *resp {
	return &resp{
		rtype: rtype,
		data:  data,
		array: nil,
	}
}

func newRespEmpty() *resp {
	return &resp{
		data:  nil,
		array: nil,
	}
}

func newRespString(val string) *resp {
	return &resp{
		rtype: respString,
		data:  []byte(val),
		array: nil,
	}
}
func newRespArray(resps []*resp) *resp {
	return &resp{
		rtype: respArray,
		data:  nil,
		array: resps,
	}
}

func newRespArrayWithCapcity(length int) *resp {
	return &resp{
		rtype: respArray,
		data:  nil,
		array: make([]*resp, length),
	}
}

func newRespInt(val int) *resp {
	s := strconv.Itoa(val)
	return &resp{
		rtype: respArray,
		data:  []byte(s),
		array: nil,
	}
}

func (r *resp) nth(pos int) *resp {
	return r.array[pos]
}

func (r *resp) isNull() bool {
	if r.rtype == respArray {
		return r.array == nil
	}
	if r.rtype == respBulk {
		return r.data == nil
	}
	return false
}

func (r *resp) replaceAll(begin int, newers []*resp) {
	copy(r.array[begin:], newers)
}

func (r *resp) replace(pos int, newer *resp) {
	r.array[pos] = newer
}

func (r *resp) slice() []*resp {
	return r.array
}

// Len represent the respArray type's length
func (r *resp) Len() int {
	return len(r.array)
}

func (r *resp) String() string {
	if r.rtype == respString || r.rtype == respBulk {
		return string(r.data)
	}
	// TODO(wayslog): 实现其他的命令的 string
	return ""
}

// RRequest is the type of a complete redis command
type RRequest struct {
	respObj *resp

	cmdType   CmdType
	batchStep int
}

// NewCommand will create new command by given args
// example:
//     NewCommand("GET", "mykey")
//     NewCommand("MGST", "mykey", "yourkey")
func NewCommand(cmd string, args ...string) *RRequest {
	respObj := newRespArrayWithCapcity(len(args) + 1)
	respObj.replace(0, newRespString(cmd))
	maxLen := len(args) + 1
	for i := 1; i < maxLen; i++ {
		respObj.replace(i, newRespString(args[i-1]))
	}
	return newRRequest(respObj)
}

func newRRequest(obj *resp) *RRequest {
	r := &RRequest{respObj: obj}
	cmd := r.Cmd()
	r.cmdType = getCmdType(cmd)
	r.batchStep = getBatchStep(cmd)
	return r
}

// Cmd get the cmd
func (rr *RRequest) Cmd() string {
	return strings.ToUpper(rr.respObj.nth(0).String())
}

// Key impl the proto.protoRequest and get the Key of redis
func (rr *RRequest) Key() []byte {
	return rr.respObj.nth(1).data
}

// IsBatch impl the proto.protoRequest and check if the command is batchable
func (rr *RRequest) IsBatch() bool {
	return rr.batchStep != defaultBatchStep
}

// Batch impl the proto.protoRequest and split the command into divided part.
func (rr *RRequest) Batch() ([]proto.Request, *proto.Response) {
	if rr.batchStep == defaultBatchStep {
		// batch but only split one
		return rr.batchOne()
	}

	return rr.batchByStep(rr.batchStep)
}

func (rr *RRequest) batchOne() ([]proto.Request, *proto.Response) {
	reqs := []proto.Request{
		proto.Request{
			Type: proto.CacheTypeRedis,
		},
	}
	reqs[0].WithProto(rr)
	response := &proto.Response{
		Type: proto.CacheTypeRedis,
	}
	return reqs, response
}

func (rr *RRequest) batchByStep(step int) ([]proto.Request, *proto.Response) {
	// NEEDTEST(wayslog): we assume that the rquest is full.

	// trim cmd
	cmd := rr.Cmd()
	mergeType := getMergeType(cmd)

	slice := rr.respObj.slice()[1:]

	items := (rr.respObj.Len() - 1) / step
	resps := make([]proto.Request, items)

	batchCmd := getBatchCmd(cmd)

	bcmdResp := newRespString(batchCmd)
	bcmdType := getCmdType(batchCmd)

	for i := 0; i < items; i++ {
		// keyPos := i*step
		// argsBegin := i*step+1
		// argsEnd := i*step + step -1
		r := newRespArrayWithCapcity(step + 1)
		r.replace(0, bcmdResp)
		r.replaceAll(1, slice[i*step:(i+1)*step])

		req := proto.Request{Type: proto.CacheTypeRedis}
		req.WithProto(&RRequest{
			respObj:   r,
			cmdType:   bcmdType,
			batchStep: defaultBatchStep,
		})
		resps[i] = req
	}

	response := &proto.Response{
		Type: proto.CacheTypeRedis,
	}
	response.WithProto(newRResponse(mergeType))
	return resps, response
}

// RResponse is the redis response protocol type.
type RResponse struct {
	respObj *resp

	mergeType MergeType
}

func newRResponse(mtype MergeType) *RResponse {
	return &RResponse{
		mergeType: mtype,
	}
}

// Merge impl the proto.Merge interface
func (rr *RResponse) Merge(subs []proto.Request) {
	switch rr.mergeType {
	case MergeTypeBasic:
		srr, ok := subs[0].Resp.Proto().(*RResponse)
		if ok {
			// TOOD(wayslog): log it
			return
		}
		rr.respObj = srr.respObj
	case MergeTypeJoin:
		rr.mergeJoin(subs)
	case MergeTypeCount:
		rr.mergeCount(subs)
	case MergeTypeOk:
		rr.mergeOk(subs)
	}
}

func (rr *RResponse) mergeJoin(subs []proto.Request) {
	if rr.respObj == nil {
		rr.respObj = newRespArrayWithCapcity(len(subs))
	}
	if rr.respObj.isNull() {
		rr.respObj.array = make([]*resp, len(subs))
	}
	for idx, sub := range subs {
		srr, ok := sub.Resp.Proto().(*RResponse)
		if !ok {
			// TODO(wayslog): log it
			continue
		}
		rr.respObj.replace(idx, srr.respObj)
	}
}

func (rr *RResponse) mergeCount(subs []proto.Request) {
	count := 0
	for _, sub := range subs {
		if err := sub.Resp.Err(); err != nil {
			// TODO(wayslog): log it
			continue
		}
		count++
	}
	rr.respObj = newRespInt(count)
}

func (rr *RResponse) mergeOk(subs []proto.Request) {
	for _, sub := range subs {
		if err := sub.Resp.Err(); err == nil {
			continue
		}
		ssr, ok := sub.Resp.Proto().(*RResponse)
		if !ok {
			continue
		}
		rr.respObj = ssr.respObj
		return
	}

	rr.respObj = newRespString("OK")
}