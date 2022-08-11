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
func DivideMapSelector(selectors ipld.Node, num int64) ([]ipld.Node, error) {
	sels := make([]ipld.Node, 0, num)
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	//{"R":{":>":{"f":{"f>":{"Links":{"r":{"$":11,">":{"|":[{"a":{">":{"@":{}}}}]},"^":1}}}}},"l":{"none":{}}}}
	if selectors.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector split rejected: selector must be a map")
	}
	if selectors == selectorparse.CommonSelector_ExploreAllRecursively {
		return nil, fmt.Errorf("selector split rejected: selector is ExploreAllRecursively")
	}
	startValue, endValue := GetStartAndEndOfRange(selectors)
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
