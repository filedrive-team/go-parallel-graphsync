package util

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"golang.org/x/xerrors"
	"regexp"
	"strconv"
	"strings"
)

// PathValidCharset is the regular expression fully matching a valid textselector
const PathValidCharset = `[- _0-9a-zA-Z\/\.]`

// Expression is a string-type input to SelectorSpecFromMulPath
type Expression string

var invalidChar = regexp.MustCompile(`[^` + PathValidCharset[1:len(PathValidCharset)-1] + `]`)

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

/*
DivideMapSelector divide a  map-range-selectors in to a list of map-range-selectors
each of which is a list of map-range-selectors
 e.g. {"R":{":>":{"f":{"f>":{"Links":{"r":{"$":11,">":{"|":[{"a":{">":{"@":{}}}}]},"^":1}}}}},"l":{"none":{}}}}
num=3 will be divided into
	-selecter1 `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":4,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":1}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
	-selecter2 `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":7,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":4}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
	-selecter3 `{"R":{":>":{"f":{"f>":{"Links":{"|":[{"r":{"$":11,">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"^":7}},{"i":{">":{"|":[{".":{}},{"a":{">":{"@":{}}}}]},"i":0}}]}}}},"l":{"none":{}}}}`
*/
func DivideMapSelector(selectors ipld.Node, num int64, linkNums int64) ([]ipld.Node, error) {
	if num <= 0 {
		return nil, fmt.Errorf("invalid number of selectors divide: %d", num)
	}
	if num == 1 {
		return []ipld.Node{selectors}, nil
	}
	sels := make([]ipld.Node, 0, num)
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	//{"R":{":>":{"f":{"f>":{"Links":{"r":{"$":11,">":{"|":[{"a":{">":{"@":{}}}}]},"^":1}}}}},"l":{"none":{}}}}
	if selectors.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector split rejected: selector must be a map")
	}
	if (selectors == selectorparse.CommonSelector_ExploreAllRecursively || selectors == selectorparse.CommonSelector_MatchAllRecursively) && linkNums <= 0 {
		return nil, fmt.Errorf("selector split rejected: selector is ExploreAllRecursively ,but linkNums <=0")
	}
	startValue, endValue := int64(-1), int64(-1)
	if selectors == selectorparse.CommonSelector_ExploreAllRecursively || selectors == selectorparse.CommonSelector_MatchAllRecursively {
		startValue, endValue = int64(0), linkNums
	} else {
		startValue, endValue = GetStartAndEndOfRange(selectors)
	}
	if startValue >= endValue {
		return nil, fmt.Errorf("selector split rejected: end field must be greater than start field in ExploreRange selector")
	}
	//fmt.Printf("start %v,end %v\n", startValue, endValue)
	ave := (endValue - startValue) / num
	//fmt.Printf("average %v \n", ave)
	var start, end int64 = startValue, 0
	for i := int64(0); i < num; i++ {
		end = start + ave
		if i == num-1 {
			end = endValue
		}
		node := ssb.ExploreRecursive(selector.RecursionLimitNone(),
			ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
				specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(start, end,
					ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
					ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
			})).Node()
		sels = append(sels, node)
		start = end
	}
	return sels, nil
}
func CanLookupByString(node datamodel.Node, key string) bool {
	_, err := node.LookupByString(key)
	if err == nil {
		return true
	}
	return false
}
func CanLookupBySegment(node datamodel.Node) bool {
	_, err := node.LookupBySegment(datamodel.PathSegmentOfString("Links"))
	if err == nil {
		return true
	}
	return false
}
func GetStartAndEndOfRange(selectors ipld.Node) (int64, int64) {
	find := false
	var startValue, endValue int64 = -1, -1
	for !find {
		var next ipld.Node
		var err1 error
		switch {
		case CanLookupByString(selectors, selector.SelectorKey_ExploreRecursive):
			next, _ = selectors.LookupByString(selector.SelectorKey_ExploreRecursive)
		case CanLookupByString(selectors, selector.SelectorKey_Sequence):
			next, _ = selectors.LookupByString(selector.SelectorKey_Sequence)
		case CanLookupByString(selectors, selector.SelectorKey_ExploreFields):
			next, _ = selectors.LookupByString(selector.SelectorKey_ExploreFields)
		case CanLookupByString(selectors, selector.SelectorKey_Fields):
			next, _ = selectors.LookupByString(selector.SelectorKey_Fields)
		case CanLookupBySegment(selectors):
			next, _ = selectors.LookupBySegment(datamodel.PathSegmentOfString("Links"))
		case CanLookupByString(selectors, selector.SelectorKey_ExploreRange):
			next, _ = selectors.LookupByString(selector.SelectorKey_ExploreRange)
			endNode, err := next.LookupByString(selector.SelectorKey_End)
			if err != nil {
				fmt.Printf("selector split rejected: start field must be present in ExploreRange selector %v", err)
				return startValue, endValue
			}
			startNode, err := next.LookupByString(selector.SelectorKey_Start)
			if err != nil {
				fmt.Printf("selector split rejected: start field must be present in ExploreRange selector %v", err)
				return startValue, endValue
			}
			startValue, err1 = startNode.AsInt()
			if err != nil {
				fmt.Printf("selector split rejected: get startValue failed %v", err1)
				return startValue, endValue
			}
			endValue, err1 = endNode.AsInt()
			if err != nil {
				fmt.Printf("selector split rejected: get endValue failed :%v", err1)
				return startValue, endValue
			}
			find = true
		case CanLookupByString(selectors, selector.SelectorKey_ExploreUnion):
			next, _ = selectors.LookupByString(selector.SelectorKey_ExploreUnion)
		case CanLookupByString(selectors, selector.SelectorKey_ExploreAll):
			next, _ = selectors.LookupByString(selector.SelectorKey_ExploreAll)
		case CanLookupByString(selectors, selector.SelectorKey_ExploreRecursiveEdge):
			next, _ = selectors.LookupByString(selector.SelectorKey_ExploreRecursiveEdge)
		default:
			find = false
		}
		if !find {
			selectors = next
		}
	}
	return startValue, endValue
}

/*
SelectorSpecFromMulPath transforms a textual path specification in the form x/y/1-3 (means 1,2,3)
into a go-ipld-prime selector-spec object. This is a short-term stop-gap on the
road to a more versatile text-based selector description mechanism. Therefore
the accepted syntax is relatively inflexible, and restricted to the members of
PathValidCharset. The parsing rules are:

	- The character `/` is a path segment separator
	- The character `-` is a range separator
	- An empty segment ( `...//...` ) and the unix-like `.` and `..` are illegal
	- Any other valid segment is treated as a key within a map, or (if applicable)
	  as an index within an array
*/
func SelectorSpecFromMulPath(path Expression, matchPath bool, optionalSubselectorAtTarget builder.SelectorSpec) (builder.SelectorSpec, error) {

	if path == "/" {
		return nil, fmt.Errorf("a standalone '/' is not a valid path")
	} else if m := invalidChar.FindStringIndex(string(path)); m != nil {
		return nil, fmt.Errorf("path string contains invalid character at offset %d", m[0])
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	ss := optionalSubselectorAtTarget
	// if nothing is given - use an exact matcher
	if ss == nil {
		ss = ssb.Matcher()
	}

	segments := strings.Split(string(path), "/")
	mulpath := false
	var start, end int64 = -1, -1
	// walk backwards wrapping the original selector recursively
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i] == "" {
			// allow one leading and one trailing '/' at most
			if i == 0 || i == len(segments)-1 {
				continue
			}
			return nil, fmt.Errorf("invalid empty segment at position %d", i)
		}

		if segments[i] == "." || segments[i] == ".." {
			return nil, fmt.Errorf("unsupported path segment '%s' at position %d", segments[i], i)
		}
		if strings.Contains(segments[i], "-") {
			var err error
			ranges := strings.Split(segments[i], "-")
			startStr, endStr := ranges[0], ranges[1]
			start, err = strconv.ParseInt(startStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid start range '%s' at position %d", startStr, i)
			}
			end, err = strconv.ParseInt(endStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid end range '%s' at position %d", endStr, i)
			}
			if start >= end {
				return nil, fmt.Errorf("range not valid '%s' at position %d", segments[i], i)
			}
			mulpath = true
		}
		if mulpath {
			ss = ssb.ExploreRecursive(selector.RecursionLimitNone(),
				ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
					specBuilder.Insert("Links", ssb.ExploreUnion(ssb.ExploreRange(start, end,
						ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
						ssb.ExploreIndex(0, ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))))
				}))

		} else {
			ss = ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments[i], ss)
			})

			if matchPath {
				ss = ssb.ExploreUnion(ssb.Matcher(), ss)
			}
		}
	}

	return ss, nil
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

func WalkUnionSelector(paths []string) (ipld.Node, error) {
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

	if t == nil {
		return nil
	}
	if len(t.children) == 0 {
		selectorSpec, err := textselector.SelectorSpecFromPath(textselector.Expression(t.segment), false, nil)
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
