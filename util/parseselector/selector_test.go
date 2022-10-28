package parseselector

import (
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
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
	selUnionRange := ssb.ExploreUnion(selRange, path3)

	selIndex := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreRecursive(selector.RecursionLimitNone(),
			ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
	})
	selUnionIndex := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreIndex(0, ssb.ExploreRecursive(selector.RecursionLimitNone(),
			ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))),
			ssb.ExploreIndex(5, ssb.ExploreRecursive(selector.RecursionLimitNone(),
				ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))))
	})

	selUnix := util.UnixFSPathSelectorSpec("dir/a.jpg", nil)

	selLeft := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
		}))

	selRecursiveRange := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreRange(0, 2, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
		}))

	selLimit := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreIndex(0, ssb.ExploreRecursive(selector.RecursionLimitDepth(10),
			ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
	})

	testCases := []struct {
		name       string
		selRes     builder.SelectorSpec
		resPaths   []ExplorePath
		notSupport bool
	}{
		{
			name:   "same-depth",
			selRes: selSameDepth,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash", Recursive: false},
				{Path: "Links/1/Hash", Recursive: true},
			},
		},
		{
			name:   "diff-depth",
			selRes: selDiffDepth,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:   "diff-depth & same-path",
			selRes: selDiffSamePath,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/0/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:   "more",
			selRes: selMore,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/0/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/0/Hash/Links/2/Hash", Recursive: true},
				{Path: "Links/2/Hash/Links/2/Hash", Recursive: true},
				{Path: "Links/4/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:   "union-union",
			selRes: selU2,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/1/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/0/Hash/Links/1/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:   "range",
			selRes: selRange,
			resPaths: []ExplorePath{
				{Path: "Links/1/Hash", Recursive: true},
				{Path: "Links/2/Hash", Recursive: true},
			},
		},
		{
			name:   "union-range",
			selRes: selUnionRange,
			resPaths: []ExplorePath{
				{Path: "Links/1/Hash", Recursive: true},
				{Path: "Links/2/Hash", Recursive: true},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:   "index",
			selRes: selIndex,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash", Recursive: true},
			},
		},
		{
			name:   "union-index",
			selRes: selUnionIndex,
			resPaths: []ExplorePath{
				{Path: "Links/0/Hash", Recursive: true},
				{Path: "Links/5/Hash", Recursive: true},
			},
		},
		{
			name:   "unixfs",
			selRes: selUnix,
			resPaths: []ExplorePath{
				{Path: "dir/a.jpg", Recursive: true, IsUnixfs: true},
			},
		},
		{
			name:       "not-support-index",
			selRes:     selLeft,
			resPaths:   nil,
			notSupport: true,
		},
		{
			name:       "not-support-range",
			selRes:     selRecursiveRange,
			resPaths:   nil,
			notSupport: true,
		},
		{
			name:       "not-support-limit",
			selRes:     selLimit,
			resPaths:   nil,
			notSupport: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(s *testing.T) {
			var str strings.Builder
			dagjson.Encode(tc.selRes.Node(), &str)
			fmt.Println(str.String())
			er := &ERContext{}
			_, err := er.ParseSelector(tc.selRes.Node())
			if err != nil {
				t.Fatal(err)
			}
			var paths []ExplorePath
			notSupport := false
			for _, ec := range er.eCtx {
				if ec.NotSupport() {
					notSupport = true
				}
				paths = append(paths, ec.Get()...)
			}
			require.Equal(s, tc.notSupport, notSupport)
			if !notSupport {
				require.Equal(s, len(tc.resPaths), len(paths))
				for _, path := range paths {
					require.Contains(s, tc.resPaths, path)
				}
			}
		})
	}

}
