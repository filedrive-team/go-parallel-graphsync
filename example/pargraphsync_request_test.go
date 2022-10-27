package example

import (
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/filedrive-team/go-parallel-graphsync/util/parseselector"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"strings"
	"testing"
	"time"
)

func TestSimpleParGraphSyncRequestManger(t *testing.T) {
	util.StartParGraphSyncRequestManger(context.TODO(), bigCarParExchange, cidlink.Link{Cid: bigCarRootCid}, globalAddrInfos)
}
func TestParGraphSyncRequestMangerSubtree(t *testing.T) {
	sel1, _ := textselector.SelectorSpecFromPath("Links/2/Hash", false, nil)
	sel2, _ := textselector.SelectorSpecFromPath("Links/3/Hash", false, nil)
	testCases := []struct {
		name   string
		sel    ipld.Node
		expect bool
	}{
		{
			name:   "Links/2/Hash",
			sel:    sel1.Node(),
			expect: true,
		},
		{
			name:   "Links/3/Hash",
			sel:    sel2.Node(),
			expect: true,
		},
	}
	for _, tc := range testCases {
		responseProgress, errors := bigCarParExchange.Request(context.TODO(), bigCarAddrInfos[0].ID, cidlink.Link{Cid: bigCarRootCid}, tc.sel)
		go func() {
			select {
			case err := <-errors:
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
		var ci cidlink.Link
		for blk := range responseProgress {
			if blk.LastBlock.Link != nil {
				ci = blk.LastBlock.Link.(cidlink.Link)
			}
		}
		time.Sleep(time.Second)
		fmt.Println("start")
		util.StartParGraphSyncRequestManger(context.TODO(), bigCarParExchange, ci, globalAddrInfos)
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
	selRange2, _ := GenerateSubRangeSelector("Links/0/Hash", 1, 4)
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
			parsedSelectors, err := parseselector.GenerateSelectors(tc.selRes)
			if err != nil {
				return
			}
			var cids []string
			for _, ne := range parsedSelectors {
				responseProgress, errors := bigCarParExchange.Request(context.TODO(), bigCarAddrInfos[0].ID, cidlink.Link{Cid: bigCarRootCid}, ne.Sel)
				go func() {
					select {
					case err := <-errors:
						if err != nil {
							t.Fatal(err)
						}
					}
				}()

				for blk := range responseProgress {
					if ne.Path == blk.Path.String() {
						fmt.Printf("edge:%v path=%s:%s \n", ne.Recursive, blk.Path.String(), blk.LastBlock.Link.String())
						cids = append(cids, blk.LastBlock.Link.String())
						if ne.Recursive {
							ci, _ := cid.Parse(blk.LastBlock.Link.String())
							util.StartParGraphSyncRequestManger(context.TODO(), bigCarParExchange, cidlink.Link{Cid: ci}, bigCarAddrInfos)
						}
					}
				}
				if !pathInPath(tc.cids, cids) {
					t.Fatal("fail to get the node")
				}
			}

		})

	}
}
