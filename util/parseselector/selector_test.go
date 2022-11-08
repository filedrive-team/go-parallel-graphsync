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
		name               string
		srcSelSpec         builder.SelectorSpec
		expectedPaths      []ExplorePath
		expectedNotSupport bool
	}{
		{
			name:       "same-depth",
			srcSelSpec: selSameDepth,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash", Recursive: false},
				{Path: "Links/1/Hash", Recursive: true},
			},
		},
		{
			name:       "diff-depth",
			srcSelSpec: selDiffDepth,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:       "diff-depth & same-path",
			srcSelSpec: selDiffSamePath,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/0/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:       "more",
			srcSelSpec: selMore,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/0/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/0/Hash/Links/2/Hash", Recursive: true},
				{Path: "Links/2/Hash/Links/2/Hash", Recursive: true},
				{Path: "Links/4/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:       "union-union",
			srcSelSpec: selU2,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash/Links/1/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/0/Hash/Links/1/Hash/Links/1/Hash", Recursive: true},
				{Path: "Links/0/Hash/Links/0/Hash", Recursive: false},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:       "range",
			srcSelSpec: selRange,
			expectedPaths: []ExplorePath{
				{Path: "Links/1/Hash", Recursive: true},
				{Path: "Links/2/Hash", Recursive: true},
			},
		},
		{
			name:       "union-range",
			srcSelSpec: selUnionRange,
			expectedPaths: []ExplorePath{
				{Path: "Links/1/Hash", Recursive: true},
				{Path: "Links/2/Hash", Recursive: true},
				{Path: "Links/3/Hash", Recursive: true},
			},
		},
		{
			name:       "index",
			srcSelSpec: selIndex,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash", Recursive: true},
			},
		},
		{
			name:       "union-index",
			srcSelSpec: selUnionIndex,
			expectedPaths: []ExplorePath{
				{Path: "Links/0/Hash", Recursive: true},
				{Path: "Links/5/Hash", Recursive: true},
			},
		},
		{
			name:       "unixfs",
			srcSelSpec: selUnix,
			expectedPaths: []ExplorePath{
				{Path: "dir/a.jpg", Recursive: true, IsUnixfs: true},
			},
		},
		{
			name:               "not-support-index",
			srcSelSpec:         selLeft,
			expectedPaths:      nil,
			expectedNotSupport: true,
		},
		{
			name:               "not-support-range",
			srcSelSpec:         selRecursiveRange,
			expectedPaths:      nil,
			expectedNotSupport: true,
		},
		{
			name:               "not-support-limit",
			srcSelSpec:         selLimit,
			expectedPaths:      nil,
			expectedNotSupport: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(s *testing.T) {
			var str strings.Builder
			dagjson.Encode(tc.srcSelSpec.Node(), &str)
			fmt.Println(str.String())
			er := &ERParseContext{}
			_, err := er.ParseSelector(tc.srcSelSpec.Node())
			if err != nil {
				t.Fatal(err)
			}
			var paths []ExplorePath
			notSupport := false
			for _, ec := range er.explorePathContexts {
				if ec.NotSupport() {
					notSupport = true
				}
				paths = append(paths, ec.Get()...)
			}
			require.Equal(s, tc.expectedNotSupport, notSupport)
			if !notSupport {
				require.Equal(s, len(tc.expectedPaths), len(paths))
				for _, path := range paths {
					require.Contains(s, tc.expectedPaths, path)
				}
			}
		})
	}

}
