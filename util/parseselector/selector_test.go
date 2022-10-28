package parseselector

import (
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestParseSelector(t *testing.T) {
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

	selU0, _ := textselector.SelectorSpecFromPath("/1/Hash/Links", false, ssb.ExploreUnion(fromPath1, fromPath2))
	selU1, _ := textselector.SelectorSpecFromPath("/0/Hash/Links", false, ssb.ExploreUnion(selU0, fromPath1))
	selU2, _ := textselector.SelectorSpecFromPath("Links", false, ssb.ExploreUnion(selU1, fromPath4))

	selRange, _ := util.GenerateSubRangeSelectorSpec("", 1, 3)
	path3, _ := util.GenerateDataSelectorSpec("Links/3/Hash", true, nil)
	selUnionRange := ssb.ExploreUnion(selRange, path3).Node()

	selIndex := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreRecursive(selector.RecursionLimitNone(),
			ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
	})
	selUnixfs := unixfsnode.UnixFSPathSelector("a.txt")

	testCases := []struct {
		name     string
		selRes   ipld.Node
		resPaths []ExplorePath
	}{
		{
			name:   "same-depth",
			selRes: selSameDepth.Node(),
			resPaths: []ExplorePath{
				{"Links/0/Hash", false, false},
				{"Links/1/Hash", true, false},
			},
		},
		{
			name:   "diff-depth",
			selRes: selDiffDepth.Node(),
			resPaths: []ExplorePath{
				{"Links/0/Hash/Links/0/Hash", false, false},
				{"Links/3/Hash", true, false},
			},
		},
		{
			name:   "diff-depth & same-path",
			selRes: selDiffSamePath.Node(),
			resPaths: []ExplorePath{
				{"Links/0/Hash/Links/0/Hash", false, false},
				{"Links/0/Hash/Links/1/Hash", true, false},
				{"Links/3/Hash", true, false},
			},
		},
		{
			name:   "more",
			selRes: selMore.Node(),
			resPaths: []ExplorePath{
				{"Links/0/Hash/Links/0/Hash", false, false},
				{"Links/0/Hash/Links/1/Hash", true, false},
				{"Links/0/Hash/Links/2/Hash", true, false},
				{"Links/2/Hash/Links/2/Hash", true, false},
				{"Links/4/Hash/Links/1/Hash", true, false},
				{"Links/3/Hash", true, false},
			},
		},
		{
			name:   "union-union",
			selRes: selU2.Node(),
			resPaths: []ExplorePath{
				{"Links/0/Hash/Links/1/Hash/Links/0/Hash", false, false},
				{"Links/0/Hash/Links/1/Hash/Links/1/Hash", true, false},
				{"Links/0/Hash/Links/0/Hash", false, false},
				{"Links/3/Hash", true, false},
			},
		},
		{
			name:   "range",
			selRes: selRange.Node(),
			resPaths: []ExplorePath{
				{"Links/1/Hash", true, false},
				{"Links/2/Hash", true, false},
			},
		},
		{
			name:   "union-range",
			selRes: selUnionRange,
			resPaths: []ExplorePath{
				{"Links/1/Hash", true, false},
				{"Links/2/Hash", true, false},
				{"Links/3/Hash", true, false},
			},
		},
		{
			name:   "index",
			selRes: selIndex.Node(),
			resPaths: []ExplorePath{
				{"Links/0/Hash", true, false},
			},
		},
		{
			name:   "unixfs",
			selRes: selUnixfs,
			resPaths: []ExplorePath{
				{"a.txt", false, true},
			},
		},
	}
	links, err := traversal.SelectLinks(selMore.Node())
	if err != nil {
		return
	}
	fmt.Println(links)
	for _, tc := range testCases {
		t.Run(tc.name, func(s *testing.T) {
			var str strings.Builder
			dagjson.Encode(tc.selRes, &str)
			fmt.Println(str.String())
			var er = &ERContext{}
			_, err := er.ParseSelector(tc.selRes)
			if err != nil {
				t.Fatal(err)
			}
			var paths []ExplorePath
			for _, ec := range er.eCtx {
				paths = append(paths, ec.Get()...)
			}
			require.Equal(s, len(tc.resPaths), len(paths))
			for _, path := range paths {
				require.Contains(s, tc.resPaths, path)
			}
		})
	}

}
