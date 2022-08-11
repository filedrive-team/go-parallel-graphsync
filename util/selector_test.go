package util

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"strings"
	"testing"
)

func TestSplitMapSelector(t *testing.T) {
	// create a selector to traverse the whole tree
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selectors := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert("Links", ssb.ExploreRange(1, 11,
				ssb.ExploreUnion(ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
		})).Node()
	splitSelector, err := DivideMapSelector(selectors, 3)
	if err != nil {
		return
	}
	for i, sel := range splitSelector {
		var s strings.Builder
		err := dagjson.Encode(sel, &s)
		if err != nil {
			fmt.Printf("Encode%v\n", err)
		}
		fmt.Printf("the %v node ,%s\n", i+1, s.String())
	}
}
