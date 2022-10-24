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
