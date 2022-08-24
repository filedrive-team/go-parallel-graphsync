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

func TestUnionSelector(t *testing.T) {
	testCases := []struct {
		name   string
		paths  []string
		result string
		expect bool
	}{
		{
			name:   "nil",
			paths:  []string{},
			result: `        `,
			expect: false,
		},
		{
			name:   "none",
			paths:  []string{""},
			result: `{".":{}}`,
			expect: true,
		},
		{
			name: "4 depth same",
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash/Links/2/Hash",
			},
			result: `{"f":{"f>":{"Links":{"f":{"f>":{"0":{"f":{"f>":{"Hash":{"f":{"f>":{"Links":{"|":[{"f":{"f>":{"0":{"f":{"f>":{"Hash":{".":{}}}}}}}},{"f":{"f>":{"1":{"f":{"f>":{"Hash":{".":{}}}}}}}},{"f":{"f>":{"2":{"f":{"f>":{"Hash":{".":{}}}}}}}}]}}}}}}}}}}}}}`,
			expect: true,
		},
		{
			name: "1 depth same",
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/1/Hash/Links/2/Hash",
			},
			result: `{"f":{"f>":{"Links":{"|":[{"f":{"f>":{"0":{"f":{"f>":{"Hash":{"f":{"f>":{"Links":{"|":[{"f":{"f>":{"0":{"f":{"f>":{"Hash":{".":{}}}}}}}},{"f":{"f>":{"1":{"f":{"f>":{"Hash":{".":{}}}}}}}}]}}}}}}}}}},{"f":{"f>":{"1":{"f":{"f>":{"Hash":{"f":{"f>":{"Links":{"f":{"f>":{"2":{"f":{"f>":{"Hash":{".":{}}}}}}}}}}}}}}}}}]}}}}`,
			expect: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			res, err := WalkUnionSelector(testCase.paths)
			if (err == nil) != testCase.expect {
				t.Fatal(err)
			}
			var s strings.Builder
			if res != nil {
				err = dagjson.Encode(res, &s)
				if (err == nil) != testCase.expect {
					t.Error(err)
					return
				}
			}
			if (s.String() == testCase.result) != testCase.expect {
				t.Errorf("not equal,`%v`\n`%v`\n", testCase.result, s.String())
			}
		})
	}
}
func TestTrie_Walk(t *testing.T) {
	trie := NewTrie()
	var paths = []string{
		"Links/0/Hash/Links/0/Hash",
		"Links/1/Hash/Links/1/Hash",
		"Links/0/Hash/Links/2/Hash",
	}
	var links [][]string
	for _, path := range paths {
		links = append(links, strings.Split(path, "/"))
	}
	for _, word := range links {
		trie.Insert(word)
	}
	f, _ := WalkUnionSelector(paths)
	fmt.Printf("%+v\n", f)
}
