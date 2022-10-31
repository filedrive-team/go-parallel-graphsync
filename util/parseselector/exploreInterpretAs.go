package parseselector

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	"github.com/ipld/go-ipld-prime/datamodel"
)

type ExploreInterpretAs struct {
	next selector.Selector // selector for element we're interested in
	adl  string            // reifier for the ADL we're interested in
}

// Interests for ExploreIndex is just the index specified by the selector node
func (s ExploreInterpretAs) Interests() []datamodel.PathSegment {
	return s.next.Interests()
}

// Explore returns the node's selector if
// the path matches the index for this selector or nil if not
func (s ExploreInterpretAs) Explore(n datamodel.Node, p datamodel.PathSegment) (selector.Selector, error) {
	return s.next, nil
}

// Decide always returns false because this is not a matcher
func (s ExploreInterpretAs) Decide(n datamodel.Node) bool {
	return false
}

// Match always returns false because this is not a matcher
func (s ExploreInterpretAs) Match(node datamodel.Node) (datamodel.Node, error) {
	return nil, nil
}

// NamedReifier indicates how this selector expects to Reify the current datamodel.Node.
func (s ExploreInterpretAs) NamedReifier() string {
	return s.adl
}

// Reifiable provides a feature detection interface on selectors to understand when
// and if Reification of the datamodel.node should be attempted when performing traversals.
type Reifiable interface {
	NamedReifier() string
}

// ParseExploreInterpretAs assembles a Selector
// from a ExploreInterpretAs selector node
func (er *ERContext) ParseExploreInterpretAs(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector spec parse rejected: selector body must be a map")
	}
	adlNode, err := n.LookupByString(selector.SelectorKey_As)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: the 'as' field must be present in ExploreInterpretAs clause")
	}
	next, err := n.LookupByString(selector.SelectorKey_Next)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: the 'next' field must be present in ExploreInterpretAs clause")
	}
	parseSelector, err := selector.ParseSelector(next)
	if err != nil {
		return nil, err
	}
	sel, err := er.ParseSelector(next)
	if err != nil {
		return nil, err
	}
	adl, err := adlNode.AsString()
	if err != nil {
		return nil, err
	}
	if adl == "unixfs" {
		er.collectUnixFSPath(parseSelector)
	}

	return ExploreInterpretAs{sel, adl}, nil
}
func (er *ERContext) collectUnixFSPath(next selector.Selector) {
	recursive, notSupport := checkNextSelector(next)
	expPath := &exploreUnixFSPathContext{
		path:       newPathFromPathSegments(er.ePc.pathSegment),
		recursive:  recursive,
		notSupport: notSupport,
	}
	// more efficient check
	collect := true
	for _, ectx := range er.eCtx {
		paths := ectx.Get()
		for _, pa := range paths {
			if pa.Path == expPath.path {
				collect = false
			}
		}
	}
	if collect {
		er.isUnixfs = true
		er.collectPath(expPath)
	}
}
