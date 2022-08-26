package example

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
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
	"github.com/ipld/go-car"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
)

func TestGraphSync2(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	err := startSomeGraphSyncServices(t, mainCtx, ServicesNum, true, "car-v2.car")
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
func TestUnionSelector(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	err := startSomeGraphSyncServices(t, mainCtx, 1, false, "big-v2.car")
	if err != nil {
		t.Fatal(err)
	}

	keyFile := path.Join(os.TempDir(), "gs-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gs, err := startGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("requester peerId=%s\n", host.ID())

	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	gs.RegisterPersistenceOption("newLinkSys", newlsys)

	// QmTTSVQrNxBvQDXevh3UvToezMw1XQ5hvTMCwpDc8SDnNT
	// Qmf5VLQUwEf4hi8iWqBWC21ws64vWW6mJs9y6tSCLunz5Y
	root, _ := cid.Parse("QmNnky18BKY3rewGy98yeVQuF2te6JLBFect4PsvAnsn53")

	servKeyFile := path.Join(os.TempDir(), "gs-key0")
	privKey, err := loadOrInitPeerKey(servKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	peerId, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9310")
	if err != nil {
		t.Fatal(err)
	}
	host.Peerstore().AddAddr(peerId, addr, peerstore.PermanentAddrTTL)
	testCases := []struct {
		name   string
		paths  []string
		expect bool
	}{
		//{
		//	name:   "nil",
		//	paths:  []string{},
		//	expect: false,
		//},
		{
			name:   "none",
			paths:  []string{""},
			expect: true,
		},
		{
			name: "4 depth same",
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
			},
			expect: true,
		},
		{
			name: "1 depth same",
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/1/Hash/Links/2/Hash",
			},
			expect: true,
		},
		{
			name: "4 depth same in 2 paths ",
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/1/Hash/Links/0/Hash",
				"Links/1/Hash/Links/1/Hash",
			},
			expect: true,
		},
		{
			name: "diff length",
			paths: []string{
				"Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/1/Hash/Links/0/Hash",
				"Links/1/Hash",
			},
			expect: true,
		},
		{
			name: "diff length and diff paths",
			paths: []string{
				"Links/0/Hash/Links/1/Hash",
				"Links/1/Hash/Links/0/Hash",
				"Links/2/Hash",
				"Links/3/Hash/Links/0/Hash",
			},
			expect: true,
		},
		{
			name: "more diff paths",
			paths: []string{
				"Links/2/Hash",
				"Links/3/Hash/Links/1/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/1/Hash/Links/0/Hash",
				"Links/1/Hash/Links/1/Hash",
				"Links/1/Hash/Links/2/Hash",
				"Links/1/Hash/Links/4/Hash",
			},
			expect: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			res, err := util.WalkUnionSelector(testCase.paths)
			if (err == nil) != testCase.expect {
				t.Fatal(err)
			}
			var s strings.Builder
			if res != nil {
				err = dagjson.Encode(res, &s)
				if (err == nil) != testCase.expect {
					t.Error(err)
					return
				}
			}
			result := comparePaths(t, gs, res, testCase.paths, peerId, cidlink.Link{Cid: root})
			if result != testCase.expect {
				t.Errorf("not equal,\n%v\n", s.String())
			}

		})
	}
}
func comparePaths(t *testing.T, gs graphsync.GraphExchange, node1 ipld.Node, paths2 []string, id peer.ID, root cidlink.Link) bool {
	responseProgress, errors := gs.Request(context.TODO(), id, root, node1)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	var paths1 []string
	for blk := range responseProgress {
		paths1 = append(paths1, blk.Path.String())
	}
	fmt.Printf("get paths %+v\n", paths1)
	for _, pa2 := range paths2 {
		have := false
		for _, pa1 := range paths1 {
			if pa1 == pa2 {
				have = true
				break
			}
		}
		if have {
			continue
		} else {
			return false
		}
	}
	return true
}
func TestCreatMorePathNode(t *testing.T) {
	ctx := context.Background()
	const unixfsChunkSize uint64 = 1 << 6
	const unixfsLinksPerLevel = 6
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
	dagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
	f, err := os.Open("car-v2.car")
	require.NoError(t, err, "unable to open fixture file")

	var buf bytes.Buffer
	tr := io.TeeReader(f, &buf)
	file := files.NewReaderFile(tr)

	// import to UnixFS
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)
	cidBuilder, _ := merkledag.PrefixForCidVersion(0)
	params := ihelper.DagBuilderParams{
		Maxlinks:   unixfsLinksPerLevel,
		RawLeaves:  false,
		CidBuilder: cidBuilder,
		Dagserv:    bufferedDS,
		NoCopy:     false,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(unixfsChunkSize)))
	require.NoError(t, err, "unable to setup dag builder")

	nd, err := balanced.Layout(db)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(nd.Cid())
	sf, err := os.Create("big-v2.car")
	err = car.WriteCar(ctx, bufferedDS, []cid.Cid{nd.Cid()}, sf)
	if err != nil {
		t.Fatal(err)
		return
	}

}
