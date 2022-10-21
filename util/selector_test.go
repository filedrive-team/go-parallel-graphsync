package util

import (
	"fmt"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"strings"
	"testing"
)

func TestTrie_Walk(t *testing.T) {
	trie := NewTrie()
	var paths = []string{
		"Links/0/Hash/Links/0/Hash",
		"Links/0/Hash/Links/2/Hash",
		"Links/1/Hash/Links/1/Hash",
	}
	var links [][]string
	for _, path := range paths {
		links = append(links, strings.Split(path, "/"))
	}
	for _, word := range links {
		trie.Insert(word)
	}
	f, _ := UnionSelector(paths)
	var s strings.Builder
	dagjson.Encode(f, &s)
	fmt.Printf("%+v\n", s.String())
}

func TestCheckIfLinkSelector(t *testing.T) {
	testCases := []struct {
		name   string
		path   string
		expect bool
	}{
		{
			name:   "Links",
			path:   "Links/0/Hash",
			expect: true,
		},
		{
			name:   "Links",
			path:   "Links/0/Hash/Links/1/Hash",
			expect: true,
		},
		{
			name:   "Err-Links",
			path:   "Links/0/Links",
			expect: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sel1, err := textselector.SelectorSpecFromPath(textselector.Expression(tc.path), false, nil)
			if err != nil {
				return
			}
			if CheckIfLinkSelector(sel1.Node()) != tc.expect {
				t.Fatalf("expect %v,but %v", tc.expect, !tc.expect)
			}
			sel2, err := textselector.SelectorSpecFromPath(textselector.Expression(tc.path), true, nil)
			if err != nil {
				return
			}
			if CheckIfLinkSelector(sel2.Node()) != tc.expect {
				t.Fatalf("expect %v,but %v", tc.expect, !tc.expect)
			}
		})
	}
}

func TestCheckIfUnixfsSelector(t *testing.T) {
	sel1 := unixfsnode.UnixFSPathSelector("a/b/c")
	sel2 := unixfsnode.UnixFSPathSelector("a/b/Links")
	sel3 := unixfsnode.UnixFSPathSelector("Links/0/Links")
	selspec, err := textselector.SelectorSpecFromPath("Links/0/Links", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	sel4 := selspec.Node()
	testCases := []struct {
		name   string
		sel    datamodel.Node
		expect bool
	}{
		{
			name:   "Unix",
			sel:    sel1,
			expect: true,
		},
		{
			name:   "Unix",
			sel:    sel2,
			expect: true,
		},
		{
			name:   "Unix",
			sel:    sel3,
			expect: true,
		},
		{
			name:   "Not-Unix",
			sel:    sel4,
			expect: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if CheckIfUnixfsSelector(tc.sel) != tc.expect {
				t.Fatalf("expect %v,but %v", tc.expect, !tc.expect)
			}
		})
	}
}
