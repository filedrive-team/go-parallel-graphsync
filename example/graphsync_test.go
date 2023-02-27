package example

import (
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
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
)

func TestGraphSync(t *testing.T) {
	var responseProgress <-chan graphsync.ResponseProgress
	var errors <-chan error

	globalParExchange.UnregisterPersistenceOption("newLinkSys")
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	if err := globalParExchange.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
		t.Fatal(err)
	}

	// create a selector to traverse the whole tree
	allSelector := selectorparse.CommonSelector_ExploreAllRecursively

	responseProgress, errors = globalParExchange.Request(context.TODO(), globalPeerIds[0], cidlink.Link{globalRoot}, allSelector)
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

func TestUnixfsPathGraphSync(t *testing.T) {
	// data from ipfs
	serverbs, err := loadCarV2Blockstore("./QmREu6imfQ38NgCuSWMX5i9Vj9UATvF2CJfPoRu49p58iz.car")
	if err != nil {
		t.Fatal(err)
	}
	root, _ := cid.Parse("QmREu6imfQ38NgCuSWMX5i9Vj9UATvF2CJfPoRu49p58iz")
	subRoot, _ := cid.Parse("QmTGrEttSZqV82rABfjc88iEGKXHzPvrRrtsH35Eu28AD2")
	sel := unixfsnode.UnixFSPathSelector("1.jpg")
	var s strings.Builder
	dagjson.Encode(sel, &s)
	t.Logf(s.String())
	mainCtx := context.TODO()
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9135, serverbs, false)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-unixfs-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gscli, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9140", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}

	host.Peerstore().AddAddr(addrInfos[0].ID, addrInfos[0].Addrs[0], peerstore.PermanentAddrTTL)

	responseProgress, errors := gscli.Request(mainCtx, addrInfos[0].ID, cidlink.Link{root}, sel)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	for blk := range responseProgress {
		t.Logf("path=%s links=%d\n", blk.Path.String(), blk.Node.Length())
	}

	rdag := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
	nd, err := rdag.Get(mainCtx, subRoot)
	if err != nil {
		t.Fatal(err)
	}
	file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
	if err != nil {
		t.Fatal(err)
	}
	filePath := path.Join(t.TempDir(), "1.jpg")
	t.Log(filePath)
	err = NodeWriteTo(file, filePath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGraphSyncFromMultiPeers(t *testing.T) {
	var errors <-chan error

	globalParExchange.UnregisterPersistenceOption("newLinkSys")
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	if err := globalParExchange.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
		t.Fatal(err)
	}

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
		_, errors = globalParExchange.Request(context.TODO(), globalPeerIds[i], cidlink.Link{globalRoot}, sels[i])
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
}

func TestSimpleUnionSelector(t *testing.T) {
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
			res, err := util.UnionPathSelector(testCase.paths, true)
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
			result := comparePaths(t, bigCarParExchange, res, testCase.paths, bigCarPeerIds[0], cidlink.Link{Cid: bigCarRootCid})
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
	return pathInPath(paths1, paths2)
}

func pathInPath(paths1, paths2 []string) bool {
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

//func TestCreatMorePathNode(t *testing.T) {
//	ctx := context.Background()
//	const unixfsChunkSize uint64 = 1024 * 200
//	const unixfsLinksPerLevel = 12
//	ds := datastore.NewMapDatastore()
//	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
//	dagService := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
//	f, err := os.Open("car-v2.car")
//	require.NoError(t, err, "unable to open fixture file")
//
//	var buf bytes.Buffer
//	tr := io.TeeReader(f, &buf)
//	file := files.NewReaderFile(tr)
//
//	// import to UnixFS
//	bufferedDS := ipldformat.NewBufferedDAG(ctx, dagService)
//	cidBuilder, _ := merkledag.PrefixForCidVersion(0)
//	params := ihelper.DagBuilderParams{
//		Maxlinks:   unixfsLinksPerLevel,
//		RawLeaves:  false,
//		CidBuilder: cidBuilder,
//		Dagserv:    bufferedDS,
//		NoCopy:     false,
//	}
//
//	db, err := params.New(chunker.NewSizeSplitter(file, int64(unixfsChunkSize)))
//	require.NoError(t, err, "unable to setup dag builder")
//
//	nd, err := balanced.Layout(db)
//	if err != nil {
//		t.Fatal(err)
//	}
//	fmt.Println(nd.Cid(), len(nd.Links()))
//	get, err := bufferedDS.Get(ctx, nd.Links()[1].Cid)
//	if err != nil {
//		return
//	}
//	fmt.Println(len(get.Links()))
//	sf, err := os.Create("big-v2.car")
//	err = car.WriteCar(ctx, bufferedDS, []cid.Cid{nd.Cid()}, sf)
//	if err != nil {
//		t.Fatal(err)
//		return
//	}
//
//}
