package util

import (
	"fmt"
	"github.com/ipfs/go-unixfsnode"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"strings"
	"testing"
)

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
	f, _ := UnionSelector(paths)
	fmt.Printf("%+v\n", f)
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
	testCases := []struct {
		name   string
		path   string
		expect bool
	}{
		{
			name:   "Unix",
			path:   "a/b/c",
			expect: true,
		},
		{
			name:   "Unix",
			path:   "a/b/Links",
			expect: true,
		},
		{
			name:   "True-Unix",
			path:   "Links/0/Links",
			expect: true,
		},
		{
			name:   "Err-Unix",
			path:   "Links/0/Links",
			expect: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expect {
				sel1 := unixfsnode.UnixFSPathSelector(tc.path)

				if CheckIfUnixfsSelector(sel1) != tc.expect {
					t.Fatalf("expect %v,but %v", tc.expect, !tc.expect)
				}
			} else {
				sel1, err := textselector.SelectorSpecFromPath(textselector.Expression(tc.path), false, nil)
				if err != nil {
					return
				}
				if CheckIfUnixfsSelector(sel1.Node()) != tc.expect {
					t.Fatalf("expect %v,but %v", tc.expect, !tc.expect)
				}
			}

		})
	}
}
