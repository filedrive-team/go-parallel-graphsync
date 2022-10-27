package parseselector

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

// ExploreRange traverses a list, and for each element in the range specified,
// will apply a next selector to those reached nodes.
type ExploreRange struct {
	next     selector.Selector // selector for element we're interested in
	start    int64
	end      int64
	interest []datamodel.PathSegment // index of element we're interested in
}

// Interests for ExploreRange are all path segments within the iteration range
func (s ExploreRange) Interests() []datamodel.PathSegment {
	return s.interest
}

// Explore returns the node's selector if
// the path matches an index in the range of this selector
func (s ExploreRange) Explore(n datamodel.Node, p datamodel.PathSegment) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_List {
		return nil, nil
	}
	index, err := p.Index()
	if err != nil {
		return nil, nil
	}
	if index < s.start || index >= s.end {
		return nil, nil
	}
	return s.next, nil
}

// Decide always returns false because this is not a matcher
func (s ExploreRange) Decide(n datamodel.Node) bool {
	return false
}

// Match always returns false because this is not a matcher
func (s ExploreRange) Match(node datamodel.Node) (datamodel.Node, error) {
	return nil, nil
}

// ParseExploreRange assembles a Selector
// from a ExploreRange selector node
func (er *ERContext) ParseExploreRange(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector spec parse rejected: selector body must be a map")
	}
	startNode, err := n.LookupByString(selector.SelectorKey_Start)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: start field must be present in ExploreRange selector")
	}
	startValue, err := startNode.AsInt()
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: start field must be a number in ExploreRange selector")
	}
	endNode, err := n.LookupByString(selector.SelectorKey_End)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: end field must be present in ExploreRange selector")
	}
	endValue, err := endNode.AsInt()
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: end field must be a number in ExploreRange selector")
	}
	if startValue >= endValue {
		return nil, fmt.Errorf("selector spec parse rejected: end field must be greater than start field in ExploreRange selector")
	}
	next, err := n.LookupByString(selector.SelectorKey_Next)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: next field must be present in ExploreRange selector")
	}

	sel, err := er.ParseSelector(next)
	if err != nil {
		return nil, err
	}
	x := ExploreRange{
		sel,
		startValue,
		endValue,
		make([]datamodel.PathSegment, 0, endValue-startValue),
	}

	for i := startValue; i < endValue; i++ {
		x.interest = append(x.interest, datamodel.PathSegmentOfInt(i))
	}
	er.collectPath(&exploreRangePathContext{
		path:  newPathFromPathSegments(er.ePc.pathSegment),
		start: startValue,
		end:   endValue,
	})
	return x, nil
}
