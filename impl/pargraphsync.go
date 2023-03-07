package impl

import (
	"context"
	"errors"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/groupreq"
	"github.com/filedrive-team/go-parallel-graphsync/requestmanger"
	"github.com/ipfs/go-graphsync"
	graphsyncImpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var log = logging.Logger("pargraphsync")

// ParallelGraphSync is an instance of a ParallelGraphSync exchange that implements
// the graphsync protocol.
type ParallelGraphSync struct {
	network    gsnet.GraphSyncNetwork
	linkSystem ipld.LinkSystem
	ctx        context.Context
	cancel     context.CancelFunc
	graphsync  graphsync.GraphExchange

	groupReqMgr *groupreq.GroupRequestManager
}

// New creates a new GraphSync Exchange on the given network,
// and the given link loader+storer.
func New(parent context.Context, network gsnet.GraphSyncNetwork,
	linkSystem ipld.LinkSystem, options ...graphsyncImpl.Option) pargraphsync.ParallelGraphExchange {
	ctx, cancel := context.WithCancel(parent)

	impl := &ParallelGraphSync{
		network:     network,
		linkSystem:  linkSystem,
		ctx:         ctx,
		cancel:      cancel,
		graphsync:   graphsyncImpl.New(parent, network, linkSystem, options...),
		groupReqMgr: groupreq.NewGroupRequestManager(),
	}

	return impl
}

// Request initiates a new GraphSync request to the given peer using the given selector spec.
func (gs *ParallelGraphSync) Request(ctx context.Context, p peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	var extNames []string
	for _, ext := range extensions {
		extNames = append(extNames, string(ext.Name))
	}
	ctx, _ = otel.Tracer("pragraphsync").Start(ctx, "request", trace.WithAttributes(
		attribute.String("peerID", p.String()),
		attribute.String("root", root.String()),
		attribute.StringSlice("extensions", extNames),
	))

	greq := groupreq.NewGroupRequest(ctx)
	if greq, loaded := gs.groupReqMgr.GetOrAdd(greq); loaded {
		subReq := groupreq.NewSubRequest(ctx, false)
		greq.Add(subReq)
		ctx = subReq.Ctx
	} else {
		ctx = greq.GetContext()
	}
	return gs.graphsync.Request(ctx, p, root, selector, extensions...)
}

// RequestMany initiates some new GraphSync requests to the given peers using the given selector spec.
func (gs *ParallelGraphSync) RequestMany(ctx context.Context, peers []peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error) {
	greq := groupreq.NewGroupRequest(ctx)
	if greq, loaded := gs.groupReqMgr.GetOrAdd(greq); !loaded {
		ctx = greq.GetContext()
	}

	rqManager := requestmanger.NewParGraphSyncRequestManger(gs, gs.ctx, peers, root, selector, extensions...)
	return rqManager.Start(ctx)
}

// RegisterIncomingRequestHook adds a hook that runs when a request is received
// If overrideDefaultValidation is set to true, then if the hook does not error,
// it is considered to have "validated" the request -- and that validation supersedes
// the normal validation of requests Graphsync does (i.e. all selectors can be accepted)
func (gs *ParallelGraphSync) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterIncomingRequestHook(hook)
}

// RegisterIncomingResponseHook adds a hook that runs when a response is received
func (gs *ParallelGraphSync) RegisterIncomingResponseHook(hook graphsync.OnIncomingResponseHook) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterIncomingResponseHook(hook)
}

// RegisterOutgoingRequestHook adds a hook that runs immediately prior to sending a new request
func (gs *ParallelGraphSync) RegisterOutgoingRequestHook(hook graphsync.OnOutgoingRequestHook) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterOutgoingRequestHook(hook)
}

// RegisterPersistenceOption registers an alternate loader/storer combo that can be substituted for the default
func (gs *ParallelGraphSync) RegisterPersistenceOption(name string, lsys ipld.LinkSystem) error {
	return gs.graphsync.RegisterPersistenceOption(name, lsys)
}

// UnregisterPersistenceOption unregisters an alternate loader/storer combo
func (gs *ParallelGraphSync) UnregisterPersistenceOption(name string) error {
	return gs.graphsync.UnregisterPersistenceOption(name)
}

// RegisterOutgoingBlockHook registers a hook that runs after each block is sent in a response
func (gs *ParallelGraphSync) RegisterOutgoingBlockHook(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterOutgoingBlockHook(hook)
}

// RegisterRequestUpdatedHook registers a hook that runs when an update to a request is received
func (gs *ParallelGraphSync) RegisterRequestUpdatedHook(hook graphsync.OnRequestUpdatedHook) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterRequestUpdatedHook(hook)
}

// RegisterOutgoingRequestProcessingListener adds a listener that gets called when an outgoing request actually begins processing (reaches
// the top of the outgoing request queue)
func (gs *ParallelGraphSync) RegisterOutgoingRequestProcessingListener(listener graphsync.OnRequestProcessingListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterOutgoingRequestProcessingListener(listener)
}

// RegisterOutgoingRequestProcessingListener adds a listener that gets called when an incoming request actually begins processing (reaches
// the top of the outgoing request queue)
func (gs *ParallelGraphSync) RegisterIncomingRequestProcessingListener(listener graphsync.OnRequestProcessingListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterIncomingRequestProcessingListener(listener)
}

// RegisterCompletedResponseListener adds a listener on the responder for completed responses
func (gs *ParallelGraphSync) RegisterCompletedResponseListener(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterCompletedResponseListener(listener)
}

// RegisterIncomingBlockHook adds a hook that runs when a block is received and validated (put in block store)
func (gs *ParallelGraphSync) RegisterIncomingBlockHook(hook graphsync.OnIncomingBlockHook) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterIncomingBlockHook(hook)
}

// RegisterRequestorCancelledListener adds a listener on the responder for
// responses cancelled by the requestor
func (gs *ParallelGraphSync) RegisterRequestorCancelledListener(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterRequestorCancelledListener(listener)
}

// RegisterBlockSentListener adds a listener for when blocks are actually sent over the wire
func (gs *ParallelGraphSync) RegisterBlockSentListener(listener graphsync.OnBlockSentListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterBlockSentListener(listener)
}

// RegisterNetworkErrorListener adds a listener for when errors occur sending data over the wire
func (gs *ParallelGraphSync) RegisterNetworkErrorListener(listener graphsync.OnNetworkErrorListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterNetworkErrorListener(listener)
}

// RegisterReceiverNetworkErrorListener adds a listener for when errors occur receiving data over the wire
func (gs *ParallelGraphSync) RegisterReceiverNetworkErrorListener(listener graphsync.OnReceiverNetworkErrorListener) graphsync.UnregisterHookFunc {
	return gs.graphsync.RegisterReceiverNetworkErrorListener(listener)
}

// Pause pauses an in progress request or response
func (gs *ParallelGraphSync) Pause(ctx context.Context, groupRequestID graphsync.RequestID) error {
	groupReq := gs.groupReqMgr.Get(groupRequestID)
	if groupReq == nil {
		return nil
	}
	var reqNotFound graphsync.RequestNotFoundErr
	var err error
	groupReq.RangeSubRequests(func(subReqId graphsync.RequestID, subReq *pargraphsync.SubRequest) bool {
		if errSub := gs.graphsync.Pause(ctx, subReqId); !errors.As(errSub, &reqNotFound) && err != nil {
			err = errSub
		}
		return true
	})
	return err
}

// Unpause unpauses a request or response that was paused
// Can also send extensions with unpause
func (gs *ParallelGraphSync) Unpause(ctx context.Context, groupRequestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	groupReq := gs.groupReqMgr.Get(groupRequestID)
	if groupReq == nil {
		return nil
	}
	var reqNotFound graphsync.RequestNotFoundErr
	var err error
	groupReq.RangeSubRequests(func(subReqId graphsync.RequestID, subReq *pargraphsync.SubRequest) bool {
		if errSub := gs.graphsync.Unpause(ctx, subReqId, extensions...); !errors.As(errSub, &reqNotFound) && err != nil {
			err = errSub
		}
		return true
	})
	return err
}

// Cancel cancels an in progress request or response
func (gs *ParallelGraphSync) Cancel(ctx context.Context, groupRequestID graphsync.RequestID) error {
	groupReq := gs.groupReqMgr.Get(groupRequestID)
	if groupReq == nil {
		return nil
	}
	var reqNotFound graphsync.RequestNotFoundErr
	var err error
	groupReq.RangeSubRequests(func(subReqId graphsync.RequestID, subReq *pargraphsync.SubRequest) bool {
		if errSub := gs.graphsync.Cancel(ctx, subReqId); !errors.As(errSub, &reqNotFound) && err != nil {
			err = errSub
		}
		return true
	})
	return err
}

// SendUpdate sends an update for an in progress request or response
func (gs *ParallelGraphSync) SendUpdate(ctx context.Context, groupRequestID graphsync.RequestID, extensions ...graphsync.ExtensionData) error {
	groupReq := gs.groupReqMgr.Get(groupRequestID)
	if groupReq == nil {
		return nil
	}
	var reqNotFound graphsync.RequestNotFoundErr
	var err error
	groupReq.RangeSubRequests(func(subReqId graphsync.RequestID, subReq *pargraphsync.SubRequest) bool {
		if errSub := gs.graphsync.SendUpdate(ctx, subReqId, extensions...); !errors.As(errSub, &reqNotFound) && err != nil {
			err = errSub
		}
		return true
	})
	return err
}

// Stats produces insight on the current state of a graphsync exchange
func (gs *ParallelGraphSync) Stats() graphsync.Stats {
	return gs.graphsync.Stats()
}

func (gs *ParallelGraphSync) GetGroupRequestBySubRequestId(subRequestID graphsync.RequestID) pargraphsync.GroupRequest {
	return gs.groupReqMgr.GetBySubRequestId(subRequestID)
}

// CancelSubRequest cancels an in progress sub request or response
func (gs *ParallelGraphSync) CancelSubRequest(ctx context.Context, subRequestID graphsync.RequestID) error {
	return gs.graphsync.Cancel(ctx, subRequestID)
}
