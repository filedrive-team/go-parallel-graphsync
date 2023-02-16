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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"math/rand"
	"testing"
	"time"
)

func TestParallelGraphSync(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	globalParExchange.UnregisterPersistenceOption("newLinkSys")
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	if err := globalParExchange.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
		t.Fatal(err)
	}

	peerIds := make([]peer.ID, 0, len(globalAddrInfos))
	for i := 0; i < len(globalAddrInfos); i++ {
		peerIds = append(peerIds, globalAddrInfos[i].ID)
	}
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	responseProgress, errors := globalParExchange.RequestMany(mainCtx, peerIds, cidlink.Link{globalRoot}, all.Node())
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
		if nd, err := blk.Node.LookupByString("Links"); err == nil {
			fmt.Printf("links=%d\n", nd.Length())
		}
	}

	// restore to a file
	if false {
		rdag := merkledag.NewDAGService(blockservice.New(membs, offline.Exchange(membs)))
		nd, err := rdag.Get(mainCtx, globalRoot)
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

// TODO: fix me
//func BenchmarkGraphSync(b *testing.B) {
//	mainCtx, cancel := context.WithCancel(context.TODO())
//	defer cancel()
//	const ServicesNum = 3
//	servbs, rootCid := CreateRandomBytesBlockStore(mainCtx, 290*1024*1024)
//	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9110, servbs, false)
//	if err != nil {
//		b.Fatal(err)
//	}
//	keyFile := path.Join(os.TempDir(), "gscli-key")
//	ds := datastore.NewMapDatastore()
//	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
//
//	host, gscli, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9120", keyFile, bs)
//	if err != nil {
//		b.Fatal(err)
//	}
//
//	gscli.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
//		//	fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
//		hookActions.UsePersistenceOption("newLinkSys")
//	})
//
//	sel1, err := util.GenerateSubRangeSelector("", 0, 100)
//	if err != nil {
//		b.Fatal(err)
//	}
//	sel2, err := util.GenerateSubRangeSelector("", 100, 200)
//	if err != nil {
//		b.Fatal(err)
//	}
//	sel3, err := util.GenerateSubRangeSelector("", 200, 300)
//	if err != nil {
//		b.Fatal(err)
//	}
//	sels := []datamodel.Node{
//		sel1,
//		sel2,
//		sel3,
//	}
//	params := make([]pargraphsync.RequestParam, 0, ServicesNum)
//	for i := 0; i < ServicesNum; i++ {
//		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)
//
//		params = append(params, pargraphsync.RequestParam{
//			PeerId:   addrInfos[i].ID,
//			Root:     cidlink.Link{rootCid},
//			Selector: sels[i],
//		})
//	}
//
//	// graphsync init
//	keyFile2 := path.Join(os.TempDir(), "gscli-key2")
//	host2, gscli2, err := startGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9121", keyFile2, bs)
//	if err != nil {
//		b.Fatal(err)
//	}
//	gscli2.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
//		//	fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
//		hookActions.UsePersistenceOption("newLinkSys")
//	})
//	host2.Peerstore().AddAddr(addrInfos[0].ID, addrInfos[0].Addrs[0], peerstore.PermanentAddrTTL)
//
//	b.Run("Parallel-Grapysync request to 3 service", func(b *testing.B) {
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			b.StopTimer()
//			gscli.UnregisterPersistenceOption("newLinkSys")
//			memds := datastore.NewMapDatastore()
//			membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
//			newlsys := storeutil.LinkSystemForBlockstore(membs)
//			if err := gscli.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
//				b.Fatal(err)
//			}
//			b.StartTimer()
//
//			ctx := context.Background()
//			responseProgress, errors := gscli.RequestMany(ctx, params)
//			go func() {
//				select {
//				case err := <-errors:
//					if err != nil {
//						b.Fatal(err)
//					}
//				}
//			}()
//			for range responseProgress {
//			}
//			has, err := membs.Has(mainCtx, rootCid)
//			if err != nil {
//				b.Fatal(err)
//			}
//			if !has {
//				b.Fatal("not pass")
//			}
//		}
//	})
//	b.Run("Grapysync request to 1 service", func(b *testing.B) {
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			b.StopTimer()
//			gscli2.UnregisterPersistenceOption("newLinkSys")
//			memds := datastore.NewMapDatastore()
//			membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
//			newlsys := storeutil.LinkSystemForBlockstore(membs)
//			if err := gscli2.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
//				b.Fatal(err)
//			}
//			b.StartTimer()
//
//			ctx := context.Background()
//			sel, err := util.GenerateSubRangeSelector("", 0, 300)
//			if err != nil {
//				b.Fatal(err)
//			}
//			responseProgress, errors := gscli2.Request(ctx, params[0].PeerId, params[0].Root, sel)
//			go func() {
//				select {
//				case err := <-errors:
//					if err != nil {
//						b.Fatal(err)
//					}
//				}
//			}()
//			for range responseProgress {
//			}
//			has, err := membs.Has(mainCtx, rootCid)
//			if err != nil {
//				b.Fatal(err)
//			}
//			if !has {
//				b.Fatal("not pass")
//			}
//		}
//	})
//	has, err := bs.Has(mainCtx, rootCid)
//	if err != nil {
//		b.Fatal(err)
//	}
//	if has {
//		b.Fatal("not pass")
//	}
//
//}
//
//func TestParallelGraphSyncExploreRecursiveLeftNode(t *testing.T) {
//	mainCtx, cancel := context.WithCancel(context.TODO())
//	defer cancel()
//	const ServicesNum = 3
//	servbs, root := CreateTestBlockstore(mainCtx)
//	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9210, servbs, true)
//	if err != nil {
//		t.Fatal(err)
//	}
//	keyFile := path.Join(os.TempDir(), "gs-prakey")
//	ds := datastore.NewMapDatastore()
//	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
//
//	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9220", keyFile, bs)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	gs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
//		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
//	})
//
//	selPath := "Links/0/Hash"
//	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
//	subsel := ssb.ExploreRecursive(selector.RecursionLimitNone(),
//		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
//			specBuilder.Insert("Links", ssb.ExploreUnion(
//				//ssb.ExploreRange(start, end, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
//				ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
//			),
//			)
//		}))
//	selSpec, err := util.GenerateDataSelectorSpec(selPath, false, subsel)
//	if err != nil {
//		t.Fatal(err)
//	}
//	sels := []datamodel.Node{
//		selSpec.Node(),
//	}
//	params := make([]pargraphsync.RequestParam, 0, ServicesNum)
//	for i := 0; i < len(sels); i++ {
//		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)
//
//		params = append(params, pargraphsync.RequestParam{
//			PeerId:   addrInfos[i].ID,
//			Root:     cidlink.Link{root},
//			Selector: sels[i],
//		})
//	}
//
//	ctx := context.Background()
//	responseProgress, errors := gs.RequestMany(ctx, params)
//	go func() {
//		select {
//		case err := <-errors:
//			if err != nil {
//				t.Fatal(err)
//			}
//		}
//	}()
//	for blk := range responseProgress {
//		fmt.Printf("path=%s \n", blk.Path.String())
//		if nd, err := blk.Node.LookupByString("Links"); err == nil {
//			fmt.Printf("links=%d\n", nd.Length())
//		}
//
//	}
//
//	// restore to a file
//	if false {
//		rdag := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
//		nd, err := rdag.Get(mainCtx, root)
//		if err != nil {
//			t.Fatal(err)
//		}
//		file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
//		if err != nil {
//			t.Fatal(err)
//		}
//		err = NodeWriteTo(file, "./src")
//		if err != nil {
//			t.Fatal(err)
//		}
//	}
//}
//
//func TestParallelGraphSyncControl(t *testing.T) {
//	mainCtx, cancel := context.WithCancel(context.TODO())
//	defer cancel()
//	const ServicesNum = 3
//	servbs, rootCid := CreateRandomBytesBlockStore(mainCtx, 300*1024*1024)
//	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9310, servbs, false)
//	if err != nil {
//		t.Fatal(err)
//	}
//	keyFile := path.Join(os.TempDir(), "gs-prakey")
//	ds := datastore.NewMapDatastore()
//	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
//
//	host, gs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9320", keyFile, bs)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	gs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
//		groupReq := gs.GetGroupRequestBySubRequestId(responseData.RequestID())
//		require.NotNil(t, groupReq)
//		t.Logf("groupRequestID=%s subRequestID=%s", groupReq.GetGroupRequestID(), responseData.RequestID())
//		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
//	})
//
//	sel1, err := util.GenerateSubRangeSelector("", 0, 100)
//	if err != nil {
//		t.Fatal(err)
//	}
//	sel2, err := util.GenerateSubRangeSelector("", 100, 200)
//	if err != nil {
//		t.Fatal(err)
//	}
//	sel3, err := util.GenerateSubRangeSelector("", 200, 300)
//	if err != nil {
//		t.Fatal(err)
//	}
//	sels := []datamodel.Node{
//		sel1,
//		sel2,
//		sel3,
//	}
//	params := make([]pargraphsync.RequestParam, 0, ServicesNum)
//	for i := 0; i < len(sels); i++ {
//		host.Peerstore().AddAddr(addrInfos[i].ID, addrInfos[i].Addrs[0], peerstore.PermanentAddrTTL)
//
//		params = append(params, pargraphsync.RequestParam{
//			PeerId:   addrInfos[i].ID,
//			Root:     cidlink.Link{rootCid},
//			Selector: sels[i],
//		})
//	}
//
//	groupRequestID := graphsync.NewRequestID()
//	cliCtx := context.WithValue(context.TODO(), pargraphsync.GroupRequestIDContextKey{}, groupRequestID)
//	responseProgress, errors := gs.RequestMany(cliCtx, params)
//	go func() {
//		for err := range errors {
//			// Occasionally, a load Link error is encountered
//			t.Logf("rootCid=%s error=%v", rootCid, err)
//		}
//	}()
//	time.Sleep(100 * time.Millisecond)
//	err = gs.Pause(cliCtx, groupRequestID)
//	if err != nil {
//		t.Fatal(err)
//	}
//	time.Sleep(100 * time.Millisecond)
//	err = gs.Unpause(cliCtx, groupRequestID)
//	if err != nil {
//		t.Fatal(err)
//	}
//	for blk := range responseProgress {
//		fmt.Printf("path=%s \n", blk.Path.String())
//		if nd, err := blk.Node.LookupByString("Links"); err == nil {
//			fmt.Printf("links=%d\n", nd.Length())
//		}
//	}
//}

func startPraGraphSyncClient(ctx context.Context, listenAddr string, keyFile string, bs blockstore.Blockstore) (host.Host, pargraphsync.ParallelGraphExchange, error) {
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
