package example

import (
	"context"
	"fmt"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"os"
	"path"
	"sync"
	"testing"
)

func TestGraphSync2(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	err := startSomeGraphSyncServices(t, mainCtx, ServicesNum, true)
	if err != nil {
		t.Fatal(err)
	}

	keyFile := path.Join(os.TempDir(), "gs-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
	//dsPath := path.Join(os.TempDir(), "gs-ds")
	//ds, err := levelds.NewDatastore(dsPath, nil)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//bs := blockstore.NewBlockstore(ds)

	host, gs, err := startGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("requester peerId=%s\n", host.ID())

	gs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})
	gs.RegisterIncomingResponseHook(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		reqId := responseData.RequestID().String()
		status := responseData.Status().String()
		fmt.Printf("RegisterIncomingResponseHook peer=%s response requestId=%s status=%s\n", p.String(), reqId, status)
	})
	gs.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		fmt.Printf("RegisterIncomingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	gs.RegisterBlockSentListener(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
		fmt.Printf("RegisterBlockSentListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	gs.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		fmt.Printf("RegisterCompletedResponseListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	gs.RegisterIncomingRequestQueuedHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
		fmt.Printf("RegisterIncomingRequestQueuedHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	gs.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		fmt.Printf("RegisterNetworkErrorListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	gs.RegisterOutgoingBlockHook(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		fmt.Printf("RegisterOutgoingBlockHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	gs.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	// uninitialized, is it a bug?
	//gs.RegisterOutgoingRequestProcessingListener(func(p peer.ID, request graphsync.RequestData, inProgressRequestCount int) {
	//	fmt.Printf("request requestId=%s\n", request.ID().String())
	//})
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	gs.RegisterPersistenceOption("newLinkSys", newlsys)

	gs.RegisterReceiverNetworkErrorListener(func(p peer.ID, err error) {
		fmt.Printf("RegisterReceiverNetworkErrorListener error=%s\n", err)
	})
	gs.RegisterRequestorCancelledListener(func(p peer.ID, request graphsync.RequestData) {
		fmt.Printf("RegisterRequestorCancelledListener request requestId=%s\n", request.ID().String())
	})
	gs.RegisterRequestUpdatedHook(func(p peer.ID, request graphsync.RequestData, updateRequest graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
		fmt.Printf("RegisterRequestUpdatedHook request requestId=%s\n", request.ID().String())
	})

	var errors <-chan error

	// QmTTSVQrNxBvQDXevh3UvToezMw1XQ5hvTMCwpDc8SDnNT
	// Qmf5VLQUwEf4hi8iWqBWC21ws64vWW6mJs9y6tSCLunz5Y
	root, _ := cid.Parse("Qmf5VLQUwEf4hi8iWqBWC21ws64vWW6mJs9y6tSCLunz5Y")

	// create a selector to traverse the whole tree
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	sels := make([]datamodel.Node, 0)
	selector1 := ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreRange(0, 3,
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
		})).Node()
	sels = append(sels, selector1)

	selector2 := ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(3, 7,
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
				ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
		})).Node()
	sels = append(sels, selector2)

	selector3 := ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(7, 11,
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
				ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
		})).Node()
	sels = append(sels, selector3)

	var wg sync.WaitGroup
	for i := 0; i < ServicesNum; i++ {
		servKeyFile := path.Join(os.TempDir(), fmt.Sprintf("gs-key%d", i))
		privKey, err := loadOrInitPeerKey(servKeyFile)
		if err != nil {
			t.Fatal(err)
		}
		peerId, err := peer.IDFromPrivateKey(privKey)
		if err != nil {
			t.Fatal(err)
		}
		addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/931%d", i))
		if err != nil {
			t.Fatal(err)
		}
		host.Peerstore().AddAddr(peerId, addr, peerstore.PermanentAddrTTL)

		_, errors = gs.Request(context.TODO(), peerId, cidlink.Link{root}, sels[i])
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-errors:
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()

	// restore to a file
	if false {
		rdag := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
		nd, err := rdag.Get(mainCtx, root)
		if err != nil {
			t.Fatal(err)
		}
		file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
		if err != nil {
			t.Fatal(err)
		}
		err = NodeWriteTo(file, "./src")
		if err != nil {
			t.Fatal(err)
		}
	}
}
