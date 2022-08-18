package example

import (
	"bytes"
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/impl"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func TestParallelGraphSync(t *testing.T) {
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

	sels := []datamodel.Node{
		GenerateSelecter(0, 3),
		GenerateSelecter(3, 7),
		GenerateSelecter(7, 11),
	}

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

func TestGraphSyncSelectorFromMulPath(t *testing.T) {
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

	sels := make([]datamodel.Node, 0)
	selector1, err := util.SelectorSpecFromMulPath("Links/0/Hash/Links/2-4", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	selector2, err := util.SelectorSpecFromMulPath("Links/0/Hash/Links/4-7", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	selector3, err := util.SelectorSpecFromMulPath("Links/0/Hash/Links/7-11", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	sels = append(sels, selector1.Node())
	sels = append(sels, selector2.Node())
	sels = append(sels, selector3.Node())

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
}

func BenchmarkGraphSync(b *testing.B) {
	//b.SetParallelism(10)
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	servbs, root := CreateRandomBytesBlockStore(mainCtx, 290*1024*1024)
	err := startSomeGraphSyncServicesByBlockStore(&testing.T{}, mainCtx, ServicesNum, servbs, false)
	if err != nil {
		b.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
	if err != nil {
		b.Fatal(err)
	}

	gs.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		//	fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		hookActions.UsePersistenceOption("newLinkSys")
	})

	sels := []datamodel.Node{
		GenerateSelecter(0, 100),
		GenerateSelecter(100, 200),
		GenerateSelecter(200, 300),
	}
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
	b.Run("request to 3 service", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			gs.UnregisterPersistenceOption("newLinkSys")
			memds := datastore.NewMapDatastore()
			membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
			newlsys := storeutil.LinkSystemForBlockstore(membs)
			if err := gs.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

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
			for range responseProgress {
			}
		}
	})
	b.Run("request to 1 service", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			gs.UnregisterPersistenceOption("newLinkSys")
			memds := datastore.NewMapDatastore()
			membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
			newlsys := storeutil.LinkSystemForBlockstore(membs)
			if err := gs.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

			ctx := context.Background()
			responseProgress, errors := gs.Request(ctx, params[0].PeerId, params[0].Root, GenerateSelecter(0, 300))
			go func() {
				select {
				case err := <-errors:
					if err != nil {
						b.Fatal(err)
					}
				}
			}()
			for range responseProgress {
			}
			has, err := membs.Has(mainCtx, root)
			if err != nil {
				b.Fatal(err)
			}
			if !has {
				b.Fatal("not pass")
			}
		}
	})
	has, err := bs.Has(mainCtx, root)
	if err != nil {
		b.Fatal(err)
	}
	if has {
		b.Fatal("not pass")
	}

}

func TestParallelGraphSyncDivideSelector(t *testing.T) {
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
	sel := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreRange(1, 11,
				ssb.ExploreUnion(ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
		})).Node()
	sels, err := util.DivideMapSelector(sel, ServicesNum, 0)
	if err != nil {
		t.Fatal(err)
	}
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
		blk.Path.String()
	}

}

func TestParallelGraphSyncWithoutRange(t *testing.T) {
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
	//ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	params := make([]pargraphsync.RequestParam, 0, ServicesNum)
	peerIds := make([]peer.ID, 0, ServicesNum)
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
		peerIds = append(peerIds, peerId)
		host.Peerstore().AddAddr(peerId, addr, peerstore.PermanentAddrTTL)
	}

	sel, err := textselector.SelectorSpecFromPath("Links/0/Hash/Links/0", true, nil)
	if err != nil {
		t.Fatal(err)
	}

	responseProgressRoot, errorsRoot := gs.Request(context.TODO(), peerIds[0], cidlink.Link{root}, sel.Node())
	go func() {
		select {
		case err := <-errorsRoot:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	var linkNums int64 = -1
	for blk := range responseProgressRoot {
		var buf strings.Builder
		dagpb.Encode(blk.Node, &buf)
		fmt.Printf("path=%s \n", blk.Path.String())
		//fmt.Println()
		if blocks.NewBlock([]byte(buf.String())).Cid() != blocks.NewBlock([]byte("")).Cid() && blocks.NewBlock([]byte(buf.String())).Cid() != root {
			//fmt.Printf(" aaaa:%v\n", blocks.NewBlock([]byte(buf.String())))
			protobuf, err := merkledag.DecodeProtobuf([]byte(buf.String()))
			if err != nil {
				return
			}
			linkNums = int64(len(protobuf.Links()))
		}
	}
	if linkNums == -1 {
		t.Fatalf("linkNums is -1")
	}
	fmt.Printf("linkNums=%d\n", linkNums)
	sels, err := util.DivideMapSelector(selectorparse.CommonSelector_ExploreAllRecursively, ServicesNum, linkNums)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < ServicesNum; i++ {
		params = append(params, pargraphsync.RequestParam{
			PeerId:   peerIds[i],
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

func CreateRandomBytesBlockStore(ctx context.Context, dataSize int) (blockstore.Blockstore, cid.Cid) {
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
	dagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	// random data
	data := make([]byte, dataSize)
	_, err := rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)

	buf := bytes.NewReader(data)
	file := files.NewReaderFile(buf)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)

	params := ihelper.DagBuilderParams{
		Maxlinks:   1024,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	// split data into 1024000 bytes size chunks then DAGify it
	db, err := params.New(chunker.NewSizeSplitter(file, int64(1024000)))
	nd, err := balanced.Layout(db)
	err = bufferedDS.Commit()

	if err != nil {
		panic(err)
	}

	return bs, nd.Cid()
}

func GenerateSelecter(start, end int64) datamodel.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(start, end,
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
				ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
		})).Node()
}