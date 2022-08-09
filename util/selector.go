package util

import (
	"fmt"
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
