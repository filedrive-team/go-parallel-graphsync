package example

import (
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

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
	selRange2, _ := util.GenerateSubRangeSelector("Links/0/Hash", 1, 4, nil)
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
