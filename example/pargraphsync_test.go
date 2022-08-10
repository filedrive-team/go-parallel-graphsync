package example

import (
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/impl"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	gsnet "github.com/ipfs/go-graphsync/network"
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
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"os"
	"path"
	"testing"
	"time"
)

func TestParallelGraphSync(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	err := startSomeGraphSyncServices(t, mainCtx, ServicesNum)
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

	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
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

	params := make([]pargraphsync.RequestParam, 0, ServicesNum)
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

		params = append(params, pargraphsync.RequestParam{
			PeerId:   peerId,
			Root:     cidlink.Link{root},
			Selector: sels[i],
		})
	}

	responseProgress, errors := gs.RequestMany(mainCtx, params)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	for blk := range responseProgress {
		fmt.Printf("path=%s \n", blk.Path.String())
	}

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

func BenchmarkGraphSync(b *testing.B) {
	fmt.Printf("BenchmarkParallelGraphSync\n ")
	b.SetParallelism(10)
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	err := startSomeGraphSyncServices(&testing.T{}, mainCtx, ServicesNum)
	if err != nil {
		b.Fatal(err)
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

	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
	if err != nil {
		b.Fatal(err)
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
			specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(7, 15,
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
				ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
		})).Node()
	selector015 := ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(0, 15,
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
				ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
		})).Node()
	sels = append(sels, selector3)
	params := make([]pargraphsync.RequestParam, 0, ServicesNum)
	for i := 0; i < ServicesNum; i++ {
		servKeyFile := path.Join(os.TempDir(), fmt.Sprintf("gs-key%d", i))
		privKey, err := loadOrInitPeerKey(servKeyFile)
		if err != nil {
			b.Fatal(err)
		}
		peerId, err := peer.IDFromPrivateKey(privKey)
		if err != nil {
			b.Fatal(err)
		}
		addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/931%d", i))
		if err != nil {
			b.Fatal(err)
		}
		host.Peerstore().AddAddr(peerId, addr, peerstore.PermanentAddrTTL)

		params = append(params, pargraphsync.RequestParam{
			PeerId:   peerId,
			Root:     cidlink.Link{root},
			Selector: sels[i],
		})
	}
	var timeCost1, timeCost3 int64
	b.Run("3 service", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			timeStart := time.Now()
			ctx := context.Background()
			responseProgress, errors := gs.RequestMany(ctx, params)
			go func() {
				select {
				case err := <-errors:
					if err != nil {
						b.Fatal(err)
					}
				}
			}()
			for blk := range responseProgress {
				fmt.Printf("path=%s \n", blk.Path.String())
			}
			timeCost3 = time.Now().Sub(timeStart).Milliseconds()
		}

	})
	b.Run("1 service", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			timeStart := time.Now()
			ctx := context.Background()
			responseProgress, errors := gs.Request(ctx, params[0].PeerId, params[0].Root, selector015)
			go func() {
				select {
				case err := <-errors:
					if err != nil {
						b.Fatal(err)
					}
				}
			}()

			for blk := range responseProgress {
				fmt.Printf("path=%s \n", blk.Path.String())
			}
			timeCost1 = time.Now().Sub(timeStart).Milliseconds()
		}

	})

	fmt.Println("1 service timeCost=", timeCost1, "ms")
	fmt.Println("3 services timeCost=", timeCost3, "ms")

}

func startPraGraphSyncClient(ctx context.Context, listenAddr string, keyFile string, bs blockstore.Blockstore) (host.Host, pargraphsync.ParallelGraphExchange, error) {
	peerkey, err := loadOrInitPeerKey(keyFile)
	if err != nil {
		return nil, nil, err
	}

	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		return nil, nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.DefaultTransports,
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, nil, err
	}

	lsys := storeutil.LinkSystemForBlockstore(bs)

	network := gsnet.NewFromLibp2pHost(host)
	exchange := impl.New(ctx, network, lsys)
	return host, exchange, nil
}
