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

	selecter2 := `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":7,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":4}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
	selecter1 := `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":4,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":1}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
	selecter3 := `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":11,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":7}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`

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
		fmt.Printf("%v \n", s.String() == selecter1 || s.String() == selecter2 || s.String() == selecter3)
	}
}

//selecter2 `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":7,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":4}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
//selecter1 `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":4,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":1}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
//selecter3 `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":11,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":7}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
