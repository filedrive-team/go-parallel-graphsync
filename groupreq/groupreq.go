package groupreq

import (
	"context"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/ipfs/go-graphsync"
	"sync"
)

func NewSubRequest(ctx context.Context, forceNew bool) *pargraphsync.SubRequest {
	requestId := graphsync.NewRequestID()
	if !forceNew {
		idFromContext := ctx.Value(graphsync.RequestIDContextKey{})
		if reqId, ok := idFromContext.(graphsync.RequestID); ok {
			requestId = reqId
		}
	}
	ctx = context.WithValue(ctx, graphsync.RequestIDContextKey{}, requestId)
	return &pargraphsync.SubRequest{
		RequestID: requestId,
		Ctx:       ctx,
	}
}

type GroupRequest struct {
	groupRequestID graphsync.RequestID
	ctx            context.Context
	subReqs        sync.Map
}

func NewGroupRequest(ctx context.Context) *GroupRequest {
	idFromContext := ctx.Value(pargraphsync.GroupRequestIDContextKey{})
	var groupRequestId graphsync.RequestID
	var ok bool
	if groupRequestId, ok = idFromContext.(graphsync.RequestID); !ok {
		groupRequestId = graphsync.NewRequestID()
		ctx = context.WithValue(ctx, pargraphsync.GroupRequestIDContextKey{}, groupRequestId)
	}
	return &GroupRequest{
		groupRequestID: groupRequestId,
		ctx:            ctx,
	}
}

func (gr *GroupRequest) GetGroupRequestID() graphsync.RequestID {
	return gr.groupRequestID
}

func (gr *GroupRequest) GetContext() context.Context {
	return gr.ctx
}

func (gr *GroupRequest) Has(subReqId graphsync.RequestID) bool {
	_, ok := gr.subReqs.Load(subReqId)
	return ok
}

func (gr *GroupRequest) Add(subReq *pargraphsync.SubRequest) {
	gr.subReqs.Store(subReq.RequestID, subReq)
}

func (gr *GroupRequest) Get(subReqId graphsync.RequestID) *pargraphsync.SubRequest {
	if v, ok := gr.subReqs.Load(subReqId); ok {
		return v.(*pargraphsync.SubRequest)
	}
	return nil
}

func (gr *GroupRequest) Delete(subReqId graphsync.RequestID) {
	gr.subReqs.Delete(subReqId)
}

func (gr *GroupRequest) RangeSubRequests(f func(subReqId graphsync.RequestID, subReq *pargraphsync.SubRequest) bool) {
	gr.subReqs.Range(func(key, value interface{}) bool {
		subReqId, ok := key.(graphsync.RequestID)
		if !ok {
			panic("key can not convert to RequestID type")
		}
		subReq, ok := value.(*pargraphsync.SubRequest)
		if !ok {
			panic("value can not convert to *SubRequest type")
		}
		return f(subReqId, subReq)
	})
}

type GroupRequestManager struct {
	reqs sync.Map
}

func NewGroupRequestManager() *GroupRequestManager {
	return &GroupRequestManager{}
}

func (grm *GroupRequestManager) Has(groupReqId graphsync.RequestID) bool {
	_, ok := grm.reqs.Load(groupReqId)
	return ok
}

func (grm *GroupRequestManager) Add(groupReq *GroupRequest) {
	grm.reqs.Store(groupReq.groupRequestID, groupReq)
}

func (grm *GroupRequestManager) Get(groupReqId graphsync.RequestID) *GroupRequest {
	if v, ok := grm.reqs.Load(groupReqId); ok {
		return v.(*GroupRequest)
	}
	return nil
}

func (grm *GroupRequestManager) Delete(groupReqId graphsync.RequestID) *GroupRequest {
	if v, ok := grm.reqs.LoadAndDelete(groupReqId); ok {
		return v.(*GroupRequest)
	}
	return nil
}

func (grm *GroupRequestManager) GetBySubRequestId(subReqId graphsync.RequestID) *GroupRequest {
	var find *GroupRequest
	grm.reqs.Range(func(key, value interface{}) bool {
		groupReq, ok := value.(*GroupRequest)
		if !ok {
			panic("value can not convert to *GroupRequest type")
		}
		if !groupReq.Has(subReqId) {
			return true
		}

		find = groupReq
		return false
	})
	return find
}

func (grm *GroupRequestManager) GetOrAdd(groupReq *GroupRequest) (gr *GroupRequest, loaded bool) {
	actual, loaded := grm.reqs.LoadOrStore(groupReq.GetGroupRequestID(), groupReq)
	return actual.(*GroupRequest), loaded
}
