package example

import (
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/pgmanager"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/filedrive-team/go-parallel-graphsync/util/parseselector"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"os"
	"path"
	"strings"
	"testing"
)

func startWithBigCar(ctx context.Context) {
	//mainCtx, cancel := context.WithCancel(context.Background())
	//defer cancel()

	addrInfos, err := startSomeGraphSyncServices(ctx, ServicesNum, 9030, false, "big-v2.car")
	if err != nil {
		panic(err)
	}
	bigCarAddrInfos = addrInfos

	keyFile := path.Join(os.TempDir(), "gs-key-big")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))
	bigCarHost, bigCarParExchange, err = startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9040", keyFile, bs)
	if err != nil {
		panic(err)
	}
	fmt.Printf("requester peerId=%s\n", bigCarHost.ID())

	bigCarParExchange.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})
	// QmTTSVQrNxBvQDXevh3UvToezMw1XQ5hvTMCwpDc8SDnNT
	// Qmf5VLQUwEf4hi8iWqBWC21ws64vWW6mJs9y6tSCLunz5Y
	bigCarRootCid, _ = cid.Parse("QmSvtt6abwrp3MybYqHHA4BdFjjuLBABXjLEVQKpMUfUU8")
	var peers []peer.ID
	for _, addrInfo := range bigCarAddrInfos {
		bigCarHost.Peerstore().AddAddr(addrInfo.ID, addrInfo.Addrs[0], peerstore.PermanentAddrTTL)
		peers = append(peers, addrInfo.ID)
	}
	parallelGraphServerManger = pgmanager.NewPeerGroupManager(peers)
}

// TODO: fix me
// since child nodes are automatically collected and synchronized in M2, this test method is no longer applicable
//func TestSimpleDivideSelector(t *testing.T) {
//	var s = util.ParGSTask{
//		Gs:           bigCarParExchange,
//		AddedTasks:   make(map[string]struct{}),
//		runningRequests: make(map[string]struct{}),
//		requestChan:  make(chan []pargraphsync.RequestParam, 1),
//		doneRequests:    make(map[string]struct{}),
//		rootCid:      cidlink.Link{Cid: bigCarRootCid},
//		peerInfos:      bigCarAddrInfos,
//	}
//
//	testCases := []struct {
//		name      string
//		links     []string
//		num       []int64
//		paths     []string
//		expectRes bool
//	}{
//		{
//			name:  "normal-true",
//			links: []string{"Links", "Links/0/Hash/Links"},
//			num:   []int64{3, 2},
//			paths: []string{
//				"Links/0/Hash/Links/0/Hash",
//				"Links/0/Hash/Links/1/Hash",
//				"Links/0/Hash",
//				"Links/1/Hash",
//				"Links/2/Hash",
//			},
//			expectRes: true,
//		},
//		{
//			name:  "normal-false",
//			links: []string{"Links", "Links/0/Hash/Links"},
//			num:   []int64{3, 2},
//			paths: []string{
//				"Links/0/Hash/Links/0/Hash",
//				"Links/0/Hash/Links/1/Hash",
//				"Links/0/Hash",
//				"Links/1/Hash",
//				"Links/3/Hash",
//			},
//			expectRes: false,
//		},
//		{
//			name:  "more-true",
//			links: []string{"Links", "Links/1/Hash/Links"},
//			num:   []int64{5, 4},
//			paths: []string{
//				"Links/1/Hash/Links/0/Hash",
//				"Links/1/Hash/Links/1/Hash",
//				"Links/1/Hash/Links/2/Hash",
//				"Links/1/Hash/Links/3/Hash",
//				"Links/0/Hash",
//				"Links/1/Hash",
//				"Links/2/Hash",
//				"Links/3/Hash",
//				"Links/4/Hash",
//			},
//			expectRes: true,
//		},
//		{
//			name:  "more-links",
//			links: []string{"Links", "Links/1/Hash/Links", "Links/2/Hash/Links"},
//			num:   []int64{5, 4, 3},
//			paths: []string{
//				"Links/0/Hash",
//				"Links/1/Hash",
//				"Links/2/Hash",
//				"Links/3/Hash",
//				"Links/4/Hash",
//				"Links/1/Hash/Links/0/Hash",
//				"Links/1/Hash/Links/1/Hash",
//				"Links/1/Hash/Links/2/Hash",
//				"Links/1/Hash/Links/3/Hash",
//				"Links/2/Hash/Links/0/Hash",
//				"Links/2/Hash/Links/1/Hash",
//				"Links/2/Hash/Links/2/Hash",
//			},
//			expectRes: true,
//		},
//	}
//
//	for _, testCase := range testCases {
//		t.Run(testCase.name, func(t *testing.T) {
//			var pathMap = make(map[string]int64)
//			for i, link := range testCase.links {
//				pathMap[link] = testCase.num[i]
//			}
//			//simulate the collection of child nodes and child node information
//			s.CollectTasks(pathMap)
//
//			s.StartRun(context.TODO())
//			if compare(s.doneRequests, testCase.paths) != testCase.expectRes {
//				t.Fatal("not equal")
//			}
//		})
//	}
//
//}

//Test whether the selector generated by the different subtrees identified by the given selector can obtain the corresponding subtree cid,
//and then the operation of splitting and obtaining can be performed
func TestSimpleParseGivenSelector(t *testing.T) {
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
	selRange2, _ := util.GenerateSubRangeSelector("Links/0/Hash", 1, 4)
	testCases := []struct {
		name     string
		selRes   ipld.Node
		resPaths []string
		cids     []string
	}{
		{
			name:   "same-depth",
			selRes: selSameDepth.Node(),
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
			},
			cids: []string{"QmX6WsRCk9ANtHasCWbMDuSWJQg7bhTaYV4Qp7sjnC89vR",
				"QmRoq4iMc92NhXyg1ei3f1CpQTg9pGEhnDjrr89ytFcjdi",
			},
		},
		{
			name:   "diff-depth",
			selRes: selDiffDepth.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/3/Hash",
			},
			cids: []string{"QmcYbUP5ifm4Vj5eBZthtY3v3vJQWp5ghDiE2FZ3EjeB3f",
				"QmYFfDQ4PXSi5jb1Vci62Tt98rsHDNDkEWc69x1CUVbpr2",
			},
		},
		{
			name:   "diff-depth & same-path",
			selRes: selDiffSamePath.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/3/Hash",
			},
			cids: []string{"QmcYbUP5ifm4Vj5eBZthtY3v3vJQWp5ghDiE2FZ3EjeB3f",
				"Qme3ANCDLAvasvCQDWH2QUD62EHHBi5mtSwdsycsJQNSiM",
				"QmYFfDQ4PXSi5jb1Vci62Tt98rsHDNDkEWc69x1CUVbpr2",
			},
		},
		{
			name:   "more",
			selRes: selMore.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
				"Links/2/Hash/Links/2/Hash",
				"Links/4/Hash/Links/1/Hash",
				"Links/3/Hash",
			},
			cids: []string{"QmcYbUP5ifm4Vj5eBZthtY3v3vJQWp5ghDiE2FZ3EjeB3f",
				"Qme3ANCDLAvasvCQDWH2QUD62EHHBi5mtSwdsycsJQNSiM",
				"QmRkRCHHLghBYFPNxEX4CBgL8wfjtpMGXgwfmQb8WN2Y4o",
				"QmWUrmjuTrZDBP2FA6FCMGc5DsQrK73VJSHPuK1y7qj9Yt",
				"QmdY1tEnccTrA8j2uuUu2h32jgnm3PLPJz9NiksPnAnXbP",
				"QmYFfDQ4PXSi5jb1Vci62Tt98rsHDNDkEWc69x1CUVbpr2",
			},
		},
		{
			name:   "range&index",
			selRes: selRange1,
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
				"Links/3/Hash",
			},
			cids: []string{"QmX6WsRCk9ANtHasCWbMDuSWJQg7bhTaYV4Qp7sjnC89vR",
				"QmYFfDQ4PXSi5jb1Vci62Tt98rsHDNDkEWc69x1CUVbpr2",
				"QmRoq4iMc92NhXyg1ei3f1CpQTg9pGEhnDjrr89ytFcjdi",
				"QmTf25Um3uU26DcTBjYkZBYFUt3oRNAXHMnbnTcDc46Lfr",
			},
		},
		{
			name:   "range",
			selRes: selRange2,
			resPaths: []string{"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
				"Links/0/Hash/Links/3/Hash",
			},
			cids: []string{
				"Qme3ANCDLAvasvCQDWH2QUD62EHHBi5mtSwdsycsJQNSiM",
				"QmRkRCHHLghBYFPNxEX4CBgL8wfjtpMGXgwfmQb8WN2Y4o",
				"QmW7SsibBcHkkeVGHzbFbhE8bacn1RpC69zvRBPrnR28wj",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var sss strings.Builder
			dagjson.Encode(tc.selRes, &sss)
			fmt.Println(sss.String())
			parsed, err := parseselector.GenerateSelectors(tc.selRes)
			if err != nil {
				return
			}
			var cids []string
			for i, ne := range parsed {
				responseProgress, errors := bigCarParExchange.Request(context.TODO(), bigCarAddrInfos[i%3].ID, cidlink.Link{Cid: bigCarRootCid}, ne.Sel)
				go func() {
					select {
					case err := <-errors:
						if err != nil {
							t.Fatal(err)
						}
					}
				}()

				for blk := range responseProgress {
					if strings.HasSuffix(blk.Path.String(), "Hash") && blk.LastBlock.Link != nil {
						//fmt.Printf("nedge path=%s:%s \n", blk.Path.String(), blk.LastBlock.Link.String())
						cids = append(cids, blk.LastBlock.Link.String())
					}
				}
			}
			if !pathInPath(cids, tc.cids) {
				t.Fatal("fail")
			}
		})

	}
}

func TestSimpleParseGivenUnixFSSelector(t *testing.T) {
	serverbs, err := loadCarV2Blockstore("./test.car")
	if err != nil {
		t.Fatal(err)
	}
	root, _ := cid.Parse("QmfDBQsFWphnYYxjdAjqnqhV1fWzH7DKKamPnC1rdXupma")
	mainCtx := context.TODO()
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9236, serverbs, false)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-unixfs-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gscli, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9241", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}
	gscli.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})

	host.Peerstore().AddAddr(addrInfos[0].ID, addrInfos[0].Addrs[0], peerstore.PermanentAddrTTL)
	//sel := util.UnixFSPathSelectorNotRecursive("1.jpg")
	//"fixtures/loremfolder/subfolder/lorem.txt"
	sel1 := util.UnixFSPathSelectorSpec("video2507292463.mp4.bak.mp4", nil)
	sel2, _ := textselector.SelectorSpecFromPath("Links/1/Hash", false, nil)
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selUnionLinksUnixfs := ssb.ExploreUnion(sel1, sel2)
	selRange1 := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreRange(1, 4,
			ssb.Matcher()))
	})
	selUnionRangeUnixfs := ssb.ExploreUnion(sel1, selRange1)
	//sel := unixfsnode.UnixFSPathSelector("1.jpg")
	var s strings.Builder
	dagjson.Encode(selUnionLinksUnixfs.Node(), &s)
	t.Logf(s.String())
	//todo more testcase
	testCases := []struct {
		name     string
		selRes   ipld.Node
		resPaths []string
		cids     []string
	}{
		{
			name:     "unixfs",
			selRes:   sel1.Node(),
			resPaths: []string{"video2507292463.mp4.bak.mp4"},
			cids: []string{
				"QmeZd6zzw3JbB2eDNwrhZpPzpYd4gLbcenJuNhw9ghoMUo",
			},
		},
		{
			name:     "unixfs-Links",
			selRes:   selUnionLinksUnixfs.Node(),
			resPaths: []string{"video2507292463.mp4.bak.mp4", "Links/1/Hash"},
			cids: []string{
				"QmeZd6zzw3JbB2eDNwrhZpPzpYd4gLbcenJuNhw9ghoMUo",
				"QmQuUub9mC28G2GG9CBL8DUDFbFCZgPvvULaL3obm6JvvF",
			},
		},
		{
			name:     "unixfs-range",
			selRes:   selUnionRangeUnixfs.Node(),
			resPaths: []string{"video2507292463.mp4.bak.mp4", "Links/1/Hash"},
			cids: []string{
				"QmeZd6zzw3JbB2eDNwrhZpPzpYd4gLbcenJuNhw9ghoMUo",
				"QmeZd6zzw3JbB2eDNwrhZpPzpYd4gLbcenJuNhw9ghoMUo",
				"QmSBk1KqHZw8Wnq2v86SApj7pg7GCVtKKrzUFd1B5Dhoe6",
				"QmQuUub9mC28G2GG9CBL8DUDFbFCZgPvvULaL3obm6JvvF",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseselector.GenerateSelectors(tc.selRes)
			if err != nil {
				return
			}
			var cids []string
			for _, ne := range parsed {
				var str strings.Builder
				dagjson.Encode(ne.Sel, &str)
				fmt.Println(str.String())
				responseProgress, errors := gscli.Request(context.TODO(), addrInfos[0].ID, cidlink.Link{Cid: root}, ne.Sel)
				go func() {
					select {
					case err := <-errors:
						if err != nil {
							t.Fatal(err)
						}
					}
				}()

				for blk := range responseProgress {
					if blk.LastBlock.Link != nil {
						fmt.Printf("path=%s\n", blk.Path.String())
						cids = append(cids, blk.LastBlock.Link.String())
					}
				}
			}
			if !pathInPath(tc.cids, cids) {
				t.Fatal("fail")
			}
		})

	}
}
