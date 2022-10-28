package parseselector

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

// ExploreIndex traverses a specific index in a list, and applies a next
// selector to the reached node.
type ExploreIndex struct {
	next     selector.Selector        // selector for element we're interested in
	interest [1]datamodel.PathSegment // index of element we're interested in
}

// Interests for ExploreIndex is just the index specified by the selector node
func (s ExploreIndex) Interests() []datamodel.PathSegment {
	return s.interest[:]
}

// Explore returns the node's selector if
// the path matches the index for this selector or nil if not
func (s ExploreIndex) Explore(n datamodel.Node, p datamodel.PathSegment) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_List {
		return nil, nil
	}
	expectedIndex, expectedErr := p.Index()
	actualIndex, actualErr := s.interest[0].Index()
	if expectedErr != nil || actualErr != nil || expectedIndex != actualIndex {
		return nil, nil
	}
	return s.next, nil
}

// Decide always returns false because this is not a matcher
func (s ExploreIndex) Decide(n datamodel.Node) bool {
	return false
}

// Match always returns false because this is not a matcher
func (s ExploreIndex) Match(node datamodel.Node) (datamodel.Node, error) {
	return nil, nil
}

// ParseExploreIndex assembles a Selector
// from a ExploreIndex selector node
func (er *ERContext) ParseExploreIndex(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector spec parse rejected: selector body must be a map")
	}
	indexNode, err := n.LookupByString(selector.SelectorKey_Index)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: index field must be present in ExploreIndex selector")
	}
	indexValue, err := indexNode.AsInt()
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: index field must be a number in ExploreIndex selector")
	}
	next, err := n.LookupByString(selector.SelectorKey_Next)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: next field must be present in ExploreIndex selector")
	}
	sel, err := er.ParseSelector(next)
	if err != nil {
		return nil, err
	}
	expPath := &exploreIndexPathContext{
		path:  newPathFromPathSegments(er.ePc.pathSegment),
		index: indexValue,
	}
	expPath.recursive, expPath.notSupport = checkNextSelector(sel)
	er.collectPath(expPath)
	return ExploreIndex{sel, [1]datamodel.PathSegment{datamodel.PathSegmentOfInt(indexValue)}}, nil
}
