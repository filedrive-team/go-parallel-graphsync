package pargraphsync

import (
	"context"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

// GroupRequestIDContextKey is used to the desired request id in context when
// initializing a request
type GroupRequestIDContextKey struct{}

type GroupRequest interface {
	GetGroupRequestID() graphsync.RequestID
	GetContext() context.Context
	Has(subRequestID graphsync.RequestID) bool
	Add(subRequest *SubRequest)
	Get(subRequestID graphsync.RequestID) *SubRequest
	Delete(subRequestID graphsync.RequestID)
	RangeSubRequests(f func(subRequestID graphsync.RequestID, subRequest *SubRequest) bool)
}

type SubRequest struct {
	RequestID graphsync.RequestID
	Ctx       context.Context
}

// ParallelGraphExchange is a protocol that can exchange IPLD graphs based on a selector
type ParallelGraphExchange interface {
	graphsync.GraphExchange

	// RequestMany initiates some new GraphSync requests to the given peers using the given selector spec.
	RequestMany(ctx context.Context, peers []peer.ID, root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) (<-chan graphsync.ResponseProgress, <-chan error)
	// GetGroupRequestBySubRequestId gets GroupRequest information by subRequestID
	GetGroupRequestBySubRequestId(subRequestID graphsync.RequestID) GroupRequest
	// CancelSubRequest cancels an in progress sub request or response
	CancelSubRequest(ctx context.Context, subRequestID graphsync.RequestID) error
}
