package explore

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"reflect"
	"strings"
	"testing"
)

func TestParseSelector(t *testing.T) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	fromPath1, _ := textselector.SelectorSpecFromPath("/0/Hash", false, nil)
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
	}{
		{
			name:   "same-depth",
			selRes: selSameDepth,
			resPaths: []string{"Links/0/Hash",
				"Links/1/Hash",
			},
		},
		{
			name:   "diff-depth",
			selRes: selDiffDepth,
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/3/Hash",
			},
		},
		{
			name:   "diff-depth & same-path",
			selRes: selDiffSamePath,
			resPaths: []string{"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/3/Hash",
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
			dagjson.Encode(tc.selRes.Node(), &str)
			fmt.Println(str.String())
			var er = &ERContext{}
			_, err := er.ParseSelector(tc.selRes.Node())
			if err != nil {
				t.Fatal(err)
			}
			var paths []string
			for _, ec := range er.eCtx {
				fmt.Println(ec.path, ec.edgesFound)
				paths = append(paths, ec.path)
			}
			if !reflect.DeepEqual(paths, tc.resPaths) {
				t.Fatal("should equal")
			}
		})
	}

}
