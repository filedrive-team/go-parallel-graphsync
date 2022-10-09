package util

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"golang.org/x/xerrors"
	"regexp"
	"strings"
)

func GetDataSelector(dps *string, matchPath bool) (datamodel.Node, error) {
	sel := selectorparse.CommonSelector_ExploreAllRecursively
	if dps != nil {

		if strings.HasPrefix(string(*dps), "{") {
			var err error
			sel, err = selectorparse.ParseJSONSelector(string(*dps))
			if err != nil {
				return nil, xerrors.Errorf("failed to parse json-selector '%s': %w", *dps, err)
			}
		} else {
			ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

			selspec, err := textselector.SelectorSpecFromPath(
				textselector.Expression(*dps), matchPath,

				ssb.ExploreRecursive(
					selector.RecursionLimitNone(),
					ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())),
				),
			)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse text-selector '%s': %w", *dps, err)
			}

			sel = selspec.Node()
			fmt.Printf("partial retrieval of datamodel-path-selector %s/*\n", *dps)
		}
	}

	return sel, nil
}

func GenerateDataSelector(dpsPath string, matchPath bool, optionalSubSel builder.SelectorSpec) (datamodel.Node, error) {
	sel := selectorparse.CommonSelector_ExploreAllRecursively
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	subselAtTarget := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())),
	)
	if optionalSubSel != nil {
		subselAtTarget = optionalSubSel
	}
	selspec, err := textselector.SelectorSpecFromPath(
		textselector.Expression(dpsPath), matchPath,
		subselAtTarget,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse text-selector '%s': %w", dpsPath, err)
	}

	sel = selspec.Node()

	return sel, nil
}

type trieNode struct {
	segment  string
	isEnding bool
	children map[string]*trieNode
}

func NewTrieNode(segment string) *trieNode {
	return &trieNode{
		segment:  segment,
		isEnding: false,
		children: make(map[string]*trieNode),
	}
}

type Trie struct {
	root *trieNode
}

func NewTrie() *Trie {
	tn := NewTrieNode("!")
	return &Trie{tn}
}

func (t *Trie) Insert(nodes []string) {
	node := t.root
	for _, code := range nodes {
		value, ok := node.children[code]
		if !ok {
			value = NewTrieNode(code)
			node.children[code] = value
		}
		value.segment = code

		node = value
	}
	node.isEnding = true
}
func PathsToTrie(paths []string) *Trie {
	trie := NewTrie()
	var links [][]string
	for _, path := range paths {
		links = append(links, strings.Split(path, "/"))
	}
	//fmt.Printf("%v\n", links)
	for _, word := range links {
		trie.Insert(word)
	}
	return trie
}

func UnionSelector(paths []string) (ipld.Node, error) {
	if len(paths) < 1 {
		return nil, fmt.Errorf("paths should not be nil")
	}
	if len(paths) == 1 {
		selectorSpec, err := textselector.SelectorSpecFromPath(textselector.Expression(paths[0]), false, nil)
		return selectorSpec.Node(), err
	}
	trieTree := PathsToTrie(paths)
	//fmt.Printf("%v", trieTree)
	sel := UnionSelectorsFromTrieNode(trieTree.root)
	//var s strings.Builder
	//err := dagjson.Encode(a.Node(), &s)
	//if err != nil {
	//	fmt.Printf("eerr:%v\n", err)
	//	return nil, err
	//}
	//fmt.Printf("result %v\n", s.String())
	return sel.Node(), nil
}
func UnionSelectorsFromTrieNode(t *trieNode) builder.SelectorSpec {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selSpec, _ := textselector.SelectorSpecFromPath("Links/0/Hash", false, ssb.ExploreRecursiveEdge())
	if t == nil {
		return nil
	}
	if len(t.children) == 0 {
		selectorSpec, err := textselector.SelectorSpecFromPath(textselector.Expression(t.segment), false, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
		if err != nil {
			return nil
		}
		return selectorSpec
	} else if len(t.children) > 1 {
		var specs []builder.SelectorSpec
		for _, v := range t.children {
			specs = append(specs, UnionSelectorsFromTrieNode(v))
		}
		selectorSpec := ssb.ExploreUnion(specs...)
		if t.segment != "!" {
			return ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
				specBuilder.Insert(t.segment, selectorSpec)
			})
		} else {
			return selectorSpec
		}

	} else {
		for _, v := range t.children {
			if t.segment != "!" {
				return ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
					specBuilder.Insert(t.segment, UnionSelectorsFromTrieNode(v))
				})
			} else {
				return UnionSelectorsFromTrieNode(v)
			}
		}

	}
	return nil
}
func Walks(t *trieNode) {
	if t == nil {
		return
	}
	for k, v := range t.children {
		fmt.Printf(`"%v"`, k)
		fmt.Println()
		Walks(v)
	}
	return
}
func NodeToPath(sel ipld.Node) (string, error) {
	var s strings.Builder
	dagjson.Encode(sel, &s)
	reg := regexp.MustCompile(`"f>":{"(?s:(.*?))"`)
	if reg == nil {
		fmt.Println("MustCompile err")
		return "", fmt.Errorf("MustCompile err")
	}

	result := reg.FindAllStringSubmatch(s.String(), -1)
	var res string
	for _, text := range result {
		res = res + text[1] + "/"
		//fmt.Printf("text[%v] = %v\n", i, text[1])
	}
	fmt.Println(res)
	return res, nil
}
func CheckIfLinkSelector(sel ipld.Node) bool {
	var s strings.Builder
	dagjson.Encode(sel, &s)
	fmt.Println(s.String())
	reg := regexp.MustCompile(`((\{"\|":\[\{"\.":\{}},)?\{"f":\{"f>":\{"Links":(\{"\|":\[\{"\.":\{}},)?\{"f":\{"f>":\{"\d+":(\{"\|":\[\{"\.":\{}},)?\{"f":\{"f>":\{"Hash":{"\.")+`)
	if reg == nil {
		fmt.Println("MustCompile err")
		return false
	}

	result := reg.FindAllStringSubmatch(s.String(), -1)
	for _, res := range result {
		for _, r := range res {
			if r != "" {
				return true
			}
		}
	}
	return false
}
func CheckIfUnixfsSelector(sel ipld.Node) bool {
	var s strings.Builder
	dagjson.Encode(sel, &s)
	reg := regexp.MustCompile(`\{".":\{}},("as":"unixfs"}+,?)+`)
	if reg == nil {
		fmt.Println("MustCompile err")
		return false
	}

	result := reg.FindAllStringSubmatch(s.String(), -1)
	if len(result) == 0 {
		return false
	}
	return true
}
