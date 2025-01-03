package util

import (
	"fmt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"golang.org/x/xerrors"
	"regexp"
	"strings"
)

func GenerateDataSelectorSpec(dpsPath string, matchPath bool, optionalSubSel builder.SelectorSpec) (selspec builder.SelectorSpec, err error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	subselAtTarget := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())),
	)
	if optionalSubSel != nil {
		subselAtTarget = optionalSubSel
	}
	selspec, err = textselector.SelectorSpecFromPath(
		textselector.Expression(dpsPath), matchPath,
		subselAtTarget,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse text-selector '%s': %w", dpsPath, err)
	}

	return selspec, nil
}

func GenerateSubRangeSelectorSpec(selPath string, matchPath bool, start, end int64, optionalSubSel builder.SelectorSpec) (builder.SelectorSpec, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	subselAtTarget := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())),
	)
	if optionalSubSel != nil {
		subselAtTarget = optionalSubSel
	}
	subsel := ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
		specBuilder.Insert("Links", ssb.ExploreRange(start, end, subselAtTarget))
	})
	return GenerateDataSelectorSpec(selPath, matchPath, subsel)
}

func GenerateSubRangeSelector(selPath string, matchPath bool, start, end int64, optionalSubSel builder.SelectorSpec) (datamodel.Node, error) {
	selSpec, err := GenerateSubRangeSelectorSpec(selPath, matchPath, start, end, optionalSubSel)
	if err != nil {
		return nil, err
	}
	return selSpec.Node(), nil
}

func GenerateLeftSubRangeSelector(selPath string, start, end int64) (datamodel.Node, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	subsel := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreUnion(
			ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
				specBuilder.Insert("Links", ssb.ExploreIndex(0,
					ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))),
				)
			}),
			ssb.ExploreFields(func(specBuilder builder.ExploreFieldsSpecBuilder) {
				specBuilder.Insert("Hash", ssb.ExploreUnion(ssb.Matcher(), ssb.ExploreAll(ssb.ExploreRecursiveEdge())))
			}),
		))
	selSpec, err := GenerateSubRangeSelectorSpec(selPath, true, start, end, subsel)
	if err != nil {
		return nil, err
	}
	return selSpec.Node(), nil
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

// UnixFSPathSelectorNotRecursive creates a selector for a file/path inside of a UnixFS directory not recursive
// if reification is setup on a link system
func UnixFSPathSelectorNotRecursive(path string) builder.SelectorSpec {
	segments := strings.Split(path, "/")
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selectorSoFar := ssb.Matcher()
	for i := len(segments) - 1; i >= 0; i-- {
		selectorSoFar = ssb.ExploreInterpretAs("unixfs",
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments[i], selectorSoFar)
			}),
		)
	}
	return selectorSoFar
}

func IsAllSelector(sel ipld.Node) bool {
	var selBuilder strings.Builder
	var allBuilder strings.Builder
	dagjson.Encode(sel, &selBuilder)
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	allSel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))
	dagjson.Encode(allSel.Node(), &allBuilder)
	if selBuilder.String() == allBuilder.String() {
		return true
	}
	return false
}

func RootLeftSelector(path string) (ipld.Node, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selSpec, err := textselector.SelectorSpecFromPath(LeftLinks, true, ssb.ExploreRecursiveEdge())
	if err != nil {
		return nil, err
	}
	fromPath, err := textselector.SelectorSpecFromPath(textselector.Expression(path), true, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
	if err != nil {
		return nil, err
	}
	return ssb.ExploreUnion(ssb.Matcher(), fromPath).Node(), nil
}

func SelectorToJson(sel ipld.Node) string {
	var selBuilder strings.Builder
	dagjson.Encode(sel, &selBuilder)
	return selBuilder.String()
}
