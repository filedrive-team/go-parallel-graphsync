package util

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
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
	splitSelector, err := DivideMapSelector(selectors, 3, 10)
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

func Test_SelectorSpecFromMulPath(t *testing.T) {
	dps := "Links/0/Hash/Links/7-11"
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	selspec, err := SelectorSpecFromMulPath(
		Expression(dps), true,
		ssb.ExploreRecursive(
			selector.RecursionLimitNone(),
			ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())),
		),
	)
	if err != nil {
		fmt.Printf("failed to parse text-selector '%s': %v", dps, err)
	}

	sel := selspec.Node()
	var s strings.Builder
	dagjson.Encode(sel, &s)
	fmt.Printf("%v\n", s.String())
}

func Test_DivideExploreAllRecursiveSelector(t *testing.T) {
	selectors := selectorparse.CommonSelector_ExploreAllRecursively
	splitSelector, err := DivideMapSelector(selectors, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	for i, sel := range splitSelector {
		var s strings.Builder
		err := dagjson.Encode(sel, &s)
		if err != nil {
			fmt.Printf("Encode%v\n", err)
		}
		fmt.Printf("the %v node ,%s\n", i+1, s.String())
		//fmt.Printf("%v \n", s.String() == selecter1 || s.String() == selecter2 || s.String() == selecter3)
	}
}
