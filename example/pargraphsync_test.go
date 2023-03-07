package example

import (
	"bytes"
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/impl"
	"github.com/filedrive-team/go-parallel-graphsync/util"
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
	"github.com/ipfs/go-unixfs"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"
)

func TestParallelGraphSync(t *testing.T) {
	//logging.SetLogLevel("parrequestmanger", "Debug")
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	globalParExchange.UnregisterPersistenceOption("newLinkSys")
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	if err := globalParExchange.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
		t.Fatal(err)
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	responseProgress, errors := globalParExchange.RequestMany(mainCtx, globalPeerIds, cidlink.Link{globalRoot}, all.Node())
	var wg sync.WaitGroup
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

	for blk := range responseProgress {
		fmt.Printf("path=%s \n", blk.Path.String())
		if nd, err := blk.Node.LookupByString("Links"); err == nil {
			fmt.Printf("links=%d\n", nd.Length())
		}
	}
	wg.Wait()

	// restore to a file
	rdag := merkledag.NewDAGService(blockservice.New(membs, offline.Exchange(membs)))
	nd, err := rdag.Get(mainCtx, globalRoot)
	if err != nil {
		t.Fatal(err)
	}
	file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
	if err != nil {
		t.Fatal(err)
	}
	filePath := path.Join(t.TempDir(), "src")
	if false {
		filePath = "src"
	}
	err = NodeWriteTo(file, filePath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParallelGraphSync2(t *testing.T) {
	//logging.SetLogLevel("parrequestmanger", "Debug")
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	addrInfos, err := startSomeGraphSyncServices(mainCtx, ServicesNum, 9050, false, "QmREu6imfQ38NgCuSWMX5i9Vj9UATvF2CJfPoRu49p58iz.car")
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gscli-key3")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gscli, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9150", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}

	gscli.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		//	fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		hookActions.UsePersistenceOption("newLinkSys")
	})
	gscli.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		t.Logf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})

	peerIds := make([]peer.ID, 0, ServicesNum)
	for i := 0; i < ServicesNum; i++ {
		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)

		peerIds = append(peerIds, addrInfos[i].ID)
	}
	root, _ := cid.Parse("QmREu6imfQ38NgCuSWMX5i9Vj9UATvF2CJfPoRu49p58iz")

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))

	t.Run("sync all", func(t *testing.T) {
		gscli.UnregisterPersistenceOption("newLinkSys")
		memds := datastore.NewMapDatastore()
		membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
		newlsys := storeutil.LinkSystemForBlockstore(membs)
		if err := gscli.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
			t.Fatal(err)
		}
		responseProgress, errors := gscli.RequestMany(mainCtx, peerIds, cidlink.Link{root}, all.Node())
		var wg sync.WaitGroup
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

		for blk := range responseProgress {
			fmt.Printf("path=%s \n", blk.Path.String())
			if nd, err := blk.Node.LookupByString("Links"); err == nil {
				fmt.Printf("links=%d\n", nd.Length())
			}
		}
		wg.Wait()

		// restore to a file
		rdag := merkledag.NewDAGService(blockservice.New(membs, offline.Exchange(membs)))
		nd, err := rdag.Get(mainCtx, root)
		if err != nil {
			t.Fatal(err)
		}
		file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
		if err != nil {
			t.Fatal(err)
		}
		filePath := path.Join(t.TempDir(), "nft")
		if false {
			filePath = "nft"
		}
		err = NodeWriteTo(file, filePath)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("sync a file by Links", func(t *testing.T) {
		gscli.UnregisterPersistenceOption("newLinkSys")
		memds := datastore.NewMapDatastore()
		membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
		newlsys := storeutil.LinkSystemForBlockstore(membs)
		if err := gscli.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
			t.Fatal(err)
		}
		sel, err := util.GenerateDataSelectorSpec("Links/3/Hash", false, nil)
		if err != nil {
			t.Fatal(err)
		}
		responseProgress, errors := gscli.RequestMany(mainCtx, peerIds, cidlink.Link{root}, sel.Node())
		var wg sync.WaitGroup
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

		for blk := range responseProgress {
			fmt.Printf("path=%s \n", blk.Path.String())
			if nd, err := blk.Node.LookupByString("Links"); err == nil {
				fmt.Printf("links=%d\n", nd.Length())
			}
		}
		wg.Wait()

		// restore to a file
		rdag := merkledag.NewDAGService(blockservice.New(membs, offline.Exchange(membs)))
		jpgRoot, _ := cid.Parse("QmRqb6EVXSrtU7wZyvTrUqDfNJLeMKyGfs1fiPb1iww8Pt")
		nd, err := rdag.Get(mainCtx, jpgRoot)
		if err != nil {
			t.Fatal(err)
		}
		file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
		if err != nil {
			t.Fatal(err)
		}
		filePath := path.Join(t.TempDir(), "3.jpg")
		if false {
			filePath = "3.jpg"
		}
		err = NodeWriteTo(file, filePath)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("sync a file by Links", func(t *testing.T) {
		gscli.UnregisterPersistenceOption("newLinkSys")
		memds := datastore.NewMapDatastore()
		membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
		newlsys := storeutil.LinkSystemForBlockstore(membs)
		if err := gscli.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
			t.Fatal(err)
		}
		sel, err := util.GenerateDataSelectorSpec("Links/5/Hash", false, nil)
		if err != nil {
			t.Fatal(err)
		}
		responseProgress, errors := gscli.RequestMany(mainCtx, peerIds, cidlink.Link{root}, sel.Node())
		var wg sync.WaitGroup
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

		for blk := range responseProgress {
			fmt.Printf("path=%s \n", blk.Path.String())
			if nd, err := blk.Node.LookupByString("Links"); err == nil {
				fmt.Printf("links=%d\n", nd.Length())
			}
		}
		wg.Wait()

		// restore to a file
		rdag := merkledag.NewDAGService(blockservice.New(membs, offline.Exchange(membs)))
		jpgRoot, _ := cid.Parse("QmfD43LXLKC1PCA9rxcPzUvMpdAuCAeJyinmKAcYnWsDUP")
		nd, err := rdag.Get(mainCtx, jpgRoot)
		if err != nil {
			t.Fatal(err)
		}
		file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
		if err != nil {
			t.Fatal(err)
		}
		filePath := path.Join(t.TempDir(), "14.jpg")
		if false {
			filePath = "14.jpg"
		}
		err = NodeWriteTo(file, filePath)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("sync a file by unixfs", func(t *testing.T) {
		gscli.UnregisterPersistenceOption("newLinkSys")
		memds := datastore.NewMapDatastore()
		membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
		newlsys := storeutil.LinkSystemForBlockstore(membs)
		if err := gscli.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
			t.Fatal(err)
		}
		sel := unixfsnode.UnixFSPathSelector("14.jpg")
		responseProgress, errors := gscli.RequestMany(mainCtx, peerIds, cidlink.Link{root}, sel)
		var wg sync.WaitGroup
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

		for blk := range responseProgress {
			fmt.Printf("path=%s \n", blk.Path.String())
			if nd, err := blk.Node.LookupByString("Links"); err == nil {
				fmt.Printf("links=%d\n", nd.Length())
			}
		}
		wg.Wait()

		// restore to a file
		rdag := merkledag.NewDAGService(blockservice.New(membs, offline.Exchange(membs)))
		jpgRoot, _ := cid.Parse("QmfD43LXLKC1PCA9rxcPzUvMpdAuCAeJyinmKAcYnWsDUP")
		nd, err := rdag.Get(mainCtx, jpgRoot)
		if err != nil {
			t.Fatal(err)
		}
		file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
		if err != nil {
			t.Fatal(err)
		}
		filePath := path.Join(t.TempDir(), "14-2.jpg")
		if false {
			filePath = "14-2.jpg"
		}
		err = NodeWriteTo(file, filePath)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestSimpleParGraphSyncRequest(t *testing.T) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	sel, _ := textselector.SelectorSpecFromPath("Links/2/Hash", false, all)
	cidNotFound, _ := cid.Parse("QmSvtt6abwrp3MybYqHHA4BdFjjuLBABXjLEVQKpMUfUU7")
	testCases := []struct {
		name string
		sel  ipld.Node
		ci   cidlink.Link
		err  error
	}{
		{
			name: "cid-notfound",
			sel:  sel.Node(),
			ci:   cidlink.Link{Cid: cidNotFound},
			err:  graphsync.RequestFailedContentNotFoundErr{},
		},
		{
			name: "cid-correct",
			sel:  sel.Node(),
			ci:   cidlink.Link{Cid: bigCarRootCid},
			err:  nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			responseProgress, errors := bigCarParExchange.RequestMany(context.TODO(), bigCarPeerIds, tc.ci, tc.sel)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case err := <-errors:
					require.Equal(t, tc.err, err)
				}
			}()

			for _ = range responseProgress {
			}
			//
			//for blk := range responseProgress {
			//	fmt.Printf("path=%s \n", blk.Path.String())
			//	if nd, err := blk.Node.LookupByString("Links"); err == nil {
			//		fmt.Printf("links=%d\n", nd.Length())
			//	}
			//}

			wg.Wait()
		})
	}
}

func TestParGraphSyncRequestMangerSubtree(t *testing.T) {
	sel1, _ := textselector.SelectorSpecFromPath("Links/2/Hash", false, nil)
	sel2, _ := textselector.SelectorSpecFromPath("Links/3/Hash", false, nil)
	testCases := []struct {
		name   string
		sel    ipld.Node
		expect error
	}{
		{
			name:   "Links/2/Hash",
			sel:    sel1.Node(),
			expect: nil,
		},
		{
			name:   "Links/3/Hash",
			sel:    sel2.Node(),
			expect: nil,
		},
	}
	for _, tc := range testCases {
		responseProgress, errors := bigCarParExchange.RequestMany(context.TODO(), bigCarPeerIds, cidlink.Link{Cid: bigCarRootCid}, tc.sel)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case err := <-errors:
				require.Equal(t, tc.expect, err)
			}
		}()

		for blk := range responseProgress {
			fmt.Printf("path=%s \n", blk.Path.String())
			if nd, err := blk.Node.LookupByString("Links"); err == nil {
				fmt.Printf("links=%d\n", nd.Length())
			}
		}
		wg.Wait()
	}

}

func TestParGraphSyncRequestMangerParseSelector(t *testing.T) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	fromPath1, _ := textselector.SelectorSpecFromPath("/0/Hash", true, nil)
	fromPath2, _ := textselector.SelectorSpecFromPath("/1/Hash", false, all)
	fromPath3, _ := textselector.SelectorSpecFromPath("/2/Hash", false, all)
	fromPath4, _ := textselector.SelectorSpecFromPath("/3/Hash", false, all)

	selu1 := ssb.ExploreUnion(fromPath1, fromPath2)
	selu2 := ssb.ExploreUnion(fromPath1, fromPath2, fromPath3)

	selSameDepth, _ := textselector.SelectorSpecFromPath("Links", false, selu1)

	seldiff1, _ := textselector.SelectorSpecFromPath("/0/Hash/Links", false, fromPath1)
	selDiffDepth, _ := textselector.SelectorSpecFromPath("Links", false, ssb.ExploreUnion(seldiff1, fromPath4))

	seldiffsame1, _ := textselector.SelectorSpecFromPath("/0/Hash/Links", false, selu1)
	seldiffsame2 := ssb.ExploreUnion(seldiffsame1, fromPath4)
	selDiffSamePath, _ := textselector.SelectorSpecFromPath("Links", false, seldiffsame2)

	selmore1, _ := textselector.SelectorSpecFromPath("/0/Hash/Links", false, selu2)
	selmore2, _ := textselector.SelectorSpecFromPath("/2/Hash/Links/2/Hash", false, all)
	selmore3, _ := textselector.SelectorSpecFromPath("/4/Hash/Links/1/Hash", false, all)
	selMore, _ := textselector.SelectorSpecFromPath("Links", false, ssb.ExploreUnion(selmore1, selmore2, selmore3, fromPath4))

	selRange1 := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreRange(1, 4,
			ssb.ExploreIndex(0, ssb.Matcher())))
	}).Node()
	selRange2, _ := util.GenerateSubRangeSelector("Links/0/Hash", false, 1, 4, nil)
	testCases := []struct {
		name     string
		sel      ipld.Node
		resPaths []string
		expect   error
	}{
		{
			name: "same-depth",
			sel:  selSameDepth.Node(),
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
			},
			expect: nil,
		},

		{
			name: "diff-depth",
			sel:  selDiffDepth.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/3/Hash",
			},
			expect: nil,
		},
		{
			name: "diff-depth & same-path",
			sel:  selDiffSamePath.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/3/Hash",
			},
			expect: nil,
		},
		{
			name: "more",
			sel:  selMore.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
				"Links/2/Hash/Links/2/Hash",
				"Links/4/Hash/Links/1/Hash",
				"Links/3/Hash",
			},
			expect: nil,
		},
		{
			name: "range&index",
			sel:  selRange1,
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
				"Links/3/Hash",
			},
			expect: nil,
		},
		{
			name: "range",
			sel:  selRange2,
			resPaths: []string{"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
				"Links/0/Hash/Links/3/Hash",
			},
			expect: nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			responseProgress, errors := bigCarParExchange.RequestMany(context.TODO(), bigCarPeerIds, cidlink.Link{Cid: bigCarRootCid}, tc.sel)
			go func() {
				select {
				case err := <-errors:
					require.Equal(t, tc.expect, err)
				}
			}()

			for blk := range responseProgress {
				fmt.Printf("path=%s \n", blk.Path.String())
				if nd, err := blk.Node.LookupByString("Links"); err == nil {
					fmt.Printf("links=%d\n", nd.Length())
				}
			}
		})

	}
}

func BenchmarkGraphSync(b *testing.B) {
	//logging.SetLogLevel("parrequestmanger", "Debug")
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	servbs, rootCid := CreateRandomBytesBlockStore(mainCtx, 290*1024*1024)
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9110, servbs, false)
	if err != nil {
		b.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gscli-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gscli, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9120", keyFile, bs)
	if err != nil {
		b.Fatal(err)
	}

	gscli.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		//	fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		hookActions.UsePersistenceOption("newLinkSys")
	})

	peerIds := make([]peer.ID, 0, ServicesNum)
	for i := 0; i < ServicesNum; i++ {
		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)

		peerIds = append(peerIds, addrInfos[i].ID)
	}

	// graphsync init
	keyFile2 := path.Join(os.TempDir(), "gscli-key2")
	host2, gscli2, err := startGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9121", keyFile2, bs)
	if err != nil {
		b.Fatal(err)
	}
	gscli2.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		//	fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		hookActions.UsePersistenceOption("newLinkSys")
	})
	host2.Peerstore().AddAddr(addrInfos[0].ID, addrInfos[0].Addrs[0], peerstore.PermanentAddrTTL)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))

	b.Run("Parallel-Graphsync request to 3 services", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			gscli.UnregisterPersistenceOption("newLinkSys")
			memds := datastore.NewMapDatastore()
			membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
			newlsys := storeutil.LinkSystemForBlockstore(membs)
			if err := gscli.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

			ctx := context.Background()
			responseProgress, errors := gscli.RequestMany(ctx, peerIds, cidlink.Link{rootCid}, all.Node())
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
	b.Run("Graphsync request to 1 service", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			gscli2.UnregisterPersistenceOption("newLinkSys")
			memds := datastore.NewMapDatastore()
			membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
			newlsys := storeutil.LinkSystemForBlockstore(membs)
			if err := gscli2.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()

			ctx := context.Background()
			responseProgress, errors := gscli2.Request(ctx, peerIds[0], cidlink.Link{rootCid}, all.Node())
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
			has, err := membs.Has(mainCtx, rootCid)
			if err != nil {
				b.Fatal(err)
			}
			if !has {
				b.Fatal("not pass")
			}
		}
	})
	has, err := bs.Has(mainCtx, rootCid)
	if err != nil {
		b.Fatal(err)
	}
	if has {
		b.Fatal("not pass")
	}

}

func TestParallelGraphSyncControl(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	servbs, rootCid := CreateRandomBytesBlockStore(mainCtx, 300*1024*1024)
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9310, servbs, false)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-prakey")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}

	gs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		groupReq := gs.GetGroupRequestBySubRequestId(responseData.RequestID())
		require.NotNil(t, groupReq)
		t.Logf("groupRequestID=%s subRequestID=%s", groupReq.GetGroupRequestID(), responseData.RequestID())
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))

	peerIds := make([]peer.ID, 0, ServicesNum)
	for i := 0; i < len(addrInfos); i++ {
		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)

		peerIds = append(peerIds, addrInfos[i].ID)
	}

	groupRequestID := graphsync.NewRequestID()
	cliCtx := context.WithValue(context.TODO(), pargraphsync.GroupRequestIDContextKey{}, groupRequestID)
	responseProgress, errors := gs.RequestMany(cliCtx, peerIds, cidlink.Link{rootCid}, all.Node())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errors {
			// Occasionally, a load Link error is encountered
			t.Logf("rootCid=%s error=%v", rootCid, err)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	err = gs.Pause(cliCtx, groupRequestID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	err = gs.Unpause(cliCtx, groupRequestID)
	if err != nil {
		t.Fatal(err)
	}
	for blk := range responseProgress {
		fmt.Printf("path=%s \n", blk.Path.String())
		if nd, err := blk.Node.LookupByString("Links"); err == nil {
			fmt.Printf("links=%d\n", nd.Length())
		}
	}
	wg.Wait()
}

func TestParallelGraphSyncCancel(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	const ServicesNum = 3
	servbs, rootCid := CreateRandomBytesBlockStore(mainCtx, 300*1024*1024)
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9310, servbs, false)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-prakey")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}

	gs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		groupReq := gs.GetGroupRequestBySubRequestId(responseData.RequestID())
		require.NotNil(t, groupReq)
		t.Logf("groupRequestID=%s subRequestID=%s", groupReq.GetGroupRequestID(), responseData.RequestID())
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))

	peerIds := make([]peer.ID, 0, ServicesNum)
	for i := 0; i < len(addrInfos); i++ {
		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)

		peerIds = append(peerIds, addrInfos[i].ID)
	}

	groupRequestID := graphsync.NewRequestID()
	cliCtx := context.WithValue(context.TODO(), pargraphsync.GroupRequestIDContextKey{}, groupRequestID)
	responseProgress, errors := gs.RequestMany(cliCtx, peerIds, cidlink.Link{rootCid}, all.Node())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errors {
			// Occasionally, a load Link error is encountered
			t.Logf("rootCid=%s error=%v", rootCid, err)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	err = gs.Cancel(cliCtx, groupRequestID)
	if err != nil {
		t.Fatal(err)
	}
	for blk := range responseProgress {
		fmt.Printf("path=%s \n", blk.Path.String())
		if nd, err := blk.Node.LookupByString("Links"); err == nil {
			fmt.Printf("links=%d\n", nd.Length())
		}
	}
	wg.Wait()
}

func startPraGraphSyncClient(
	ctx context.Context,
	listenAddr string,
	keyFile string,
	bs blockstore.Blockstore,
) (host.Host, pargraphsync.ParallelGraphExchange, error) {
	peerkey, err := util.LoadOrInitPeerKey(keyFile)
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

	cidBuilder, _ := unixFSCidBuilder()
	params := ihelper.DagBuilderParams{
		Maxlinks:   1024,
		RawLeaves:  true,
		CidBuilder: cidBuilder,
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

//                                       O
//                                       |
//                                       O
//                                   /   |   \
//                                 /     |     \
//                               O       O       O
//                             / | \   / | \   / | \
//                            O  O  O O  O  O O  O  O
func CreateTestBlockstore(ctx context.Context) (blockstore.Blockstore, cid.Cid) {
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
	dagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	list := make([]*merkledag.ProtoNode, 0, 9)
	for i := 0; i < 9; i++ {
		str := fmt.Sprintf("it's node%d ", i)
		fileNd := merkledag.NodeWithData(unixfs.FilePBData([]byte(str), uint64(len(str))))
		list = append(list, fileNd)
	}
	fileList := make([]*merkledag.ProtoNode, 0, 3)
	for i := 0; i < 3; i++ {
		nd := unixfs.EmptyFileNode()
		links := make([]*ipldformat.Link, 0, 3)
		for index := i * 3; index < i*3+3; index++ {
			lk, err := ipldformat.MakeLink(list[index])
			if err != nil {
				panic(err)
			}
			links = append(links, lk)
		}
		nd.SetLinks(links)
		fileList = append(fileList, nd)
	}
	dirNd := unixfs.EmptyDirNode()
	links := make([]*ipldformat.Link, 0, 3)
	for i := 0; i < 3; i++ {
		lk, err := ipldformat.MakeLink(fileList[i])
		if err != nil {
			panic(err)
		}
		lk.Name = fmt.Sprintf("file%d", i)
		links = append(links, lk)
	}
	dirNd.SetLinks(links)

	rootNd := unixfs.EmptyDirNode()
	lk, err := ipldformat.MakeLink(dirNd)
	if err != nil {
		panic(err)
	}
	lk.Name = "dir"
	rootNd.SetLinks([]*ipldformat.Link{lk})

	nds := make([]ipldformat.Node, 0, 20)
	for _, nd := range list {
		nds = append(nds, nd)
	}
	for _, nd := range fileList {
		nds = append(nds, nd)
	}
	nds = append(nds, dirNd, rootNd)
	dagService.AddMany(ctx, nds)
	return bs, rootNd.Cid()
}
