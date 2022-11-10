package example

import (
	"context"
	"github.com/filedrive-team/go-parallel-graphsync/requestmanger"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSimpleParGraphSyncRequestManger(t *testing.T) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	sel, _ := textselector.SelectorSpecFromPath("Links/2/Hash", false, all)
	ctx, cancel := context.WithCancel(context.Background())
	go parallelGraphServerManger.RecordDelay(ctx, time.Second*5)
	err := requestmanger.StartPraGraphSync(ctx, bigCarParExchange, sel.Node(), cidlink.Link{Cid: bigCarRootCid}, parallelGraphServerManger)
	require.Equal(t, nil, err)
	time.Sleep(time.Second * 10) //for testing delay
	cancel()
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
		err := requestmanger.StartPraGraphSync(context.TODO(), bigCarParExchange, tc.sel, cidlink.Link{Cid: bigCarRootCid}, parallelGraphServerManger)
		require.Equal(t, tc.expect, err)
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
	selRange2, _ := util.GenerateSubRangeSelector("Links/0/Hash", 1, 4)
	testCases := []struct {
		name     string
		selRes   ipld.Node
		resPaths []string
		expect   error
	}{
		{
			name:   "same-depth",
			selRes: selSameDepth.Node(),
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
			},
			expect: nil,
		},

		{
			name:   "diff-depth",
			selRes: selDiffDepth.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/3/Hash",
			},
			expect: nil,
		},
		{
			name:   "diff-depth & same-path",
			selRes: selDiffSamePath.Node(),
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/3/Hash",
			},
			expect: nil,
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
			expect: nil,
		},
		{
			name:   "range&index",
			selRes: selRange1,
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
				"Links/3/Hash",
			},
			expect: nil,
		},
		{
			name:   "range",
			selRes: selRange2,
			resPaths: []string{"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
				"Links/0/Hash/Links/3/Hash",
			},
			expect: nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := requestmanger.StartPraGraphSync(context.TODO(), bigCarParExchange, tc.selRes, cidlink.Link{Cid: bigCarRootCid}, parallelGraphServerManger)
			require.Equal(t, tc.expect, err)
		})

	}
}
