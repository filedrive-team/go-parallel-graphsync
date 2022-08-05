package pargraphsync

import (
	"context"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
)

// RequestParam describes param about the request
type RequestParam struct {
	PeerId     peer.ID
	Root       ipld.Link
	Selector   ipld.Node
	Extensions []graphsync.ExtensionData
}

// ParallelGraphExchange is a protocol that can exchange IPLD graphs based on a selector
type ParallelGraphExchange interface {
	graphsync.GraphExchange

	// RequestMany initiates some new GraphSync requests to the given peers of param group using the given selector spec.
	RequestMany(ctx context.Context, reqParams []RequestParam) (<-chan graphsync.ResponseProgress, <-chan error)
}
