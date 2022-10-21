package util

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"strings"
)

type trieNode struct {
	segment  string
	isEnding bool
	children map[string]*trieNode
}

func newTrieNode(segment string) *trieNode {
	return &trieNode{
		segment:  segment,
		isEnding: false,
		children: make(map[string]*trieNode),
	}
}

type trie struct {
	root     *trieNode
	isUnixfs bool
}

func newTrie(isUnixfs bool) *trie {
	root := newTrieNode("!")
	return &trie{
		root:     root,
		isUnixfs: isUnixfs,
	}
}

func newTrieFromPath(paths []string, isUnixfs bool) *trie {
	t := newTrie(isUnixfs)
	for _, path := range paths {
		t.InsertPath(path)
	}
	return t
}

func (t *trie) InsertPath(path string) {
	nodes := strings.Split(path, "/")
	node := t.root
	for _, code := range nodes {
		value, ok := node.children[code]
		if !ok {
			value = newTrieNode(code)
			node.children[code] = value
		}
		value.segment = code

		node = value
	}
	node.isEnding = true
}

func (t *trie) ToSelector() builder.SelectorSpec {
	return t.unionSelectorsFromTrieNode(t.root)
}

func (t *trie) unionSelectorsFromTrieNode(nd *trieNode) builder.SelectorSpec {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	switch len(nd.children) {
	case 0:
		selSpec, _ := textselector.SelectorSpecFromPath(LeftLinks, false, ssb.ExploreRecursiveEdge())
		var selectorSpec builder.SelectorSpec
		if t.isUnixfs {
			selectorSpec = UnixFSPathSelectorSpec(nd.segment, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
		} else {
			var err error
			selectorSpec, err = textselector.SelectorSpecFromPath(textselector.Expression(nd.segment), false, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
			if err != nil {
				panic(err)
				return nil
			}
		}
		return selectorSpec
	case 1:
		for _, v := range nd.children {
			if nd.segment != "!" {
				return ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
					specBuilder.Insert(nd.segment, t.unionSelectorsFromTrieNode(v))
				})
			} else {
				return t.unionSelectorsFromTrieNode(v)
			}
		}
	default:
		var specs []builder.SelectorSpec
		for _, v := range nd.children {
			specs = append(specs, t.unionSelectorsFromTrieNode(v))
		}
		return ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
			specBuilder.Insert(nd.segment, ssb.ExploreUnion(specs...))
		})
	}
	return nil
}

func (t *trie) Walks(visit func(name string, nd *trieNode) bool) {
	t.walks(t.root, visit)
}

func (t *trie) walks(nd *trieNode, visit func(name string, nd *trieNode) bool) {
	if !visit(nd.segment, nd) {
		return
	}
	for _, v := range nd.children {
		t.walks(v, visit)
	}
	return
}

// UnixFSPathSelectorSpec creates a selector for a file/path inside of a UnixFS directory
// if reification is setup on a link system
func UnixFSPathSelectorSpec(path string, optionalSubselectorAtTarget builder.SelectorSpec) builder.SelectorSpec {
	segments := strings.Split(path, "/")
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	ss := optionalSubselectorAtTarget
	// if nothing is given - use an exact matcher
	if ss == nil {
		ss = ssb.Matcher()
	}
	selectorSoFar := ssb.ExploreInterpretAs("unixfs", ss)
	for i := len(segments) - 1; i >= 0; i-- {
		selectorSoFar = ssb.ExploreInterpretAs("unixfs",
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments[i], selectorSoFar)
			}),
		)
	}
	return selectorSoFar
}

func UnionPathSelector(paths []string, isUnixfs bool) (ipld.Node, error) {
	if len(paths) < 1 {
		return nil, fmt.Errorf("paths should not be nil")
	}
	if len(paths) == 1 {
		if isUnixfs {
			return UnixFSPathSelectorSpec(paths[0], nil).Node(), nil
		} else {
			selectorSpec, err := textselector.SelectorSpecFromPath(textselector.Expression(paths[0]), false, nil)
			return selectorSpec.Node(), err
		}
	}
	trieTree := newTrieFromPath(paths, isUnixfs)
	sel := trieTree.ToSelector()
	if sel == nil {
		return nil, fmt.Errorf("selector is nil")
	}
	return sel.Node(), nil
}
