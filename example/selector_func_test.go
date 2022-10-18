package example

import (
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/filedrive-team/go-parallel-graphsync/util/explore"
	"github.com/ipfs/go-graphsync"
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

func startWithBigCar() (pargraphsync.ParallelGraphExchange, []peer.AddrInfo) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	bs, err := loadCarV2Blockstore("big-v2.car")
	if err != nil {
		panic(err)
	}
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, 3, 9820, bs, false)
	if err != nil {
		panic(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-key9720")
	host, pgs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9720", keyFile, bs)
	if err != nil {
		panic(err)
	}

	//this is for testing result if right , so other Register is not important.
	pgs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})
	for _, addr := range addrInfos {
		host.Peerstore().AddAddr(addr.ID, addr.Addrs[0], peerstore.PermanentAddrTTL)
	}
	return pgs, addrInfos
}

func TestSimpleDivideSelector(t *testing.T) {
	var s = util.ParGSTask{
		Gs:           bigCarParExchange,
		AddedTasks:   make(map[string]struct{}),
		StartedTasks: make(map[string]struct{}),
		RunningTasks: make(chan util.Tasks, 1),
		DoneTasks:    make(map[string]struct{}),
		RootCid:      cidlink.Link{Cid: bigCarRootCid},
		PeerIds:      bigCarAddrInfos,
	}

	testCases := []struct {
		name      string
		links     []string
		num       []int64
		paths     []string
		expectRes bool
	}{
		{
			name:  "normal-true",
			links: []string{"Links", "Links/0/Hash/Links"},
			num:   []int64{3, 2},
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
			},
			expectRes: true,
		},
		{
			name:  "normal-false",
			links: []string{"Links", "Links/0/Hash/Links"},
			num:   []int64{3, 2},
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/3/Hash",
			},
			expectRes: false,
		},
		{
			name:  "more-true",
			links: []string{"Links", "Links/1/Hash/Links"},
			num:   []int64{5, 4},
			paths: []string{
				"Links/1/Hash/Links/0/Hash",
				"Links/1/Hash/Links/1/Hash",
				"Links/1/Hash/Links/2/Hash",
				"Links/1/Hash/Links/3/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
				"Links/3/Hash",
				"Links/4/Hash",
			},
			expectRes: true,
		},
		{
			name:  "more-true",
			links: []string{"Links", "Links/1/Hash/Links", "Links/2/Hash/Links"},
			num:   []int64{5, 4, 3},
			paths: []string{
				"Links/2/Hash/Links/0/Hash",
				"Links/2/Hash/Links/1/Hash",
				"Links/2/Hash/Links/2/Hash",
				"Links/1/Hash/Links/0/Hash",
				"Links/1/Hash/Links/1/Hash",
				"Links/1/Hash/Links/2/Hash",
				"Links/1/Hash/Links/3/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
				"Links/3/Hash",
				"Links/4/Hash",
			},
			expectRes: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var pathMap = make(map[string]int64)
			for i, link := range testCase.links {
				pathMap[link] = testCase.num[i]
			}
			s.CollectTasks(pathMap)
			s.StartRun(context.TODO())
			if compare(s.DoneTasks, testCase.paths) != testCase.expectRes {
				t.Fatal("not equal")
			}
		})
	}

}

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

	testCases := []struct {
		name     string
		selRes   builder.SelectorSpec
		resPaths []string
		cids     []string
	}{
		{
			name:   "same-depth",
			selRes: selSameDepth,
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
			},
			cids: []string{"QmX6WsRCk9ANtHasCWbMDuSWJQg7bhTaYV4Qp7sjnC89vR",
				"QmRoq4iMc92NhXyg1ei3f1CpQTg9pGEhnDjrr89ytFcjdi",
			},
		},
		{
			name:   "diff-depth",
			selRes: selDiffDepth,
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/3/Hash",
			},
			cids: []string{"QmcYbUP5ifm4Vj5eBZthtY3v3vJQWp5ghDiE2FZ3EjeB3f",
				"QmYFfDQ4PXSi5jb1Vci62Tt98rsHDNDkEWc69x1CUVbpr2",
			},
		},
		{
			name:   "diff-depth & same-path",
			selRes: selDiffSamePath,
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
			selRes: selMore,
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
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			edge, nedge, err := explore.GenerateSelectors(tc.selRes.Node())
			if err != nil {
				return
			}
			var cids []string
			for i, ne := range nedge {
				responseProgress, errors := bigCarParExchange.Request(context.TODO(), bigCarAddrInfos[i%3].ID, cidlink.Link{Cid: bigCarRootCid}, ne)
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
			for _, ne := range edge {
				responseProgress, errors := bigCarParExchange.Request(context.TODO(), bigCarAddrInfos[0].ID, cidlink.Link{Cid: bigCarRootCid}, ne)
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
						//fmt.Printf("edge path=%s:%s \n", blk.Path.String(), blk.LastBlock.Link.String())
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

func compare(doneTasks map[string]struct{}, paths2 []string) bool {
	for _, pa2 := range paths2 {
		have := false
		for pa1 := range doneTasks {
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
