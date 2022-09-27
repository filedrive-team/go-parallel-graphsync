package explore

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	"github.com/ipld/go-ipld-prime/datamodel"
)

// ExploreRecursive traverses some structure recursively.
// To guide this exploration, it uses a "sequence", which is another Selector
// tree; some leaf node in this sequence should contain an ExploreRecursiveEdge
// selector, which denotes the place recursion should occur.
//
// In implementation, whenever evaluation reaches an ExploreRecursiveEdge marker
// in the recursion sequence's Selector tree, the implementation logically
// produces another new Selector which is a copy of the original
// ExploreRecursive selector, but with a decremented maxDepth parameter, and
// continues evaluation thusly.
//
// It is not valid for an ExploreRecursive selector's sequence to contain
// no instances of ExploreRecursiveEdge; it *is* valid for it to contain
// more than one ExploreRecursiveEdge.
//
// ExploreRecursive can contain a nested ExploreRecursive!
// This is comparable to a nested for-loop.
// In these cases, any ExploreRecursiveEdge instance always refers to the
// nearest parent ExploreRecursive (in other words, ExploreRecursiveEdge can
// be thought of like the 'continue' statement, or end of a for-loop body;
// it is *not* a 'goto' statement).
//
// Be careful when using ExploreRecursive with a large maxDepth parameter;
// it can easily cause very large traversals (especially if used in combination
// with selectors like ExploreAll inside the sequence).
type ExploreRecursive struct {
	sequence selector.Selector   // selector for element we're interested in
	current  selector.Selector   // selector to apply to the current node
	limit    RecursionLimit      // the limit for this recursive selector
	stopAt   *selector.Condition // a condition for not exploring the node or children
}

// RecursionLimit is a union type that captures all data about the recursion
// limit (both its type and data specific to the type)
type RecursionLimit struct {
	mode  selector.RecursionLimit_Mode
	depth int64
}

// Mode returns the type for this recursion limit
func (rl RecursionLimit) Mode() selector.RecursionLimit_Mode {
	return rl.mode
}

// Depth returns the depth for a depth recursion limit, or 0 otherwise
func (rl RecursionLimit) Depth() int64 {
	if rl.mode != selector.RecursionLimit_Depth {
		return 0
	}
	return rl.depth
}

// Interests for ExploreRecursive is empty (meaning traverse everything)
func (s ExploreRecursive) Interests() []datamodel.PathSegment {
	return s.current.Interests()
}

// Explore returns the node's selector for all fields
func (s ExploreRecursive) Explore(n datamodel.Node, p datamodel.PathSegment) (selector.Selector, error) {
	// Check any stopAt conditions right away.
	if s.stopAt != nil {
		target, err := n.LookupBySegment(p)
		if err != nil {
			return nil, err
		}
		if s.stopAt.Match(target) {
			return nil, nil
		}
	}

	// Fence against edge case: if the next selector is a recursion edge, nope, we're out.
	//  (This is only reachable if a recursion contains nothing but an edge -- which is probably somewhat rare,
	//   because it's certainly rather useless -- but it's not explicitly rejected as a malformed selector during compile, either, so it must be handled.)
	if _, ok := s.current.(selector.ExploreRecursiveEdge); ok {
		return nil, nil
	}
	// Apply the current selector clause.  (This could be midway through something resembling the initially specified sequence.)
	nextSelector, _ := s.current.Explore(n, p)

	// We have to wrap the nextSelector yielded by the current clause in recursion information before returning it,
	//  so that future levels of recursion (as well as their limits) can continue to operate correctly.
	if nextSelector == nil {
		return nil, nil
	}
	limit := s.limit
	if !s.hasRecursiveEdge(nextSelector) {
		return ExploreRecursive{s.sequence, nextSelector, limit, s.stopAt}, nil
	}
	switch limit.mode {
	case selector.RecursionLimit_Depth:
		if limit.depth < 2 {
			return s.replaceRecursiveEdge(nextSelector, nil), nil
		}
		return ExploreRecursive{s.sequence, s.replaceRecursiveEdge(nextSelector, s.sequence), RecursionLimit{selector.RecursionLimit_Depth, limit.depth - 1}, s.stopAt}, nil
	case selector.RecursionLimit_None:
		return ExploreRecursive{s.sequence, s.replaceRecursiveEdge(nextSelector, s.sequence), limit, s.stopAt}, nil
	default:
		panic("Unsupported recursion limit type")
	}
}

func (s ExploreRecursive) hasRecursiveEdge(nextSelector selector.Selector) bool {
	_, isRecursiveEdge := nextSelector.(selector.ExploreRecursiveEdge)
	if isRecursiveEdge {
		return true
	}
	exploreUnion, isUnion := nextSelector.(selector.ExploreUnion)
	if isUnion {
		for _, selector := range exploreUnion.Members {
			if s.hasRecursiveEdge(selector) {
				return true
			}
		}
	}
	return false
}

func (s ExploreRecursive) replaceRecursiveEdge(nextSelector selector.Selector, replacement selector.Selector) selector.Selector {
	_, isRecursiveEdge := nextSelector.(selector.ExploreRecursiveEdge)
	if isRecursiveEdge {
		return replacement
	}
	exploreUnion, isUnion := nextSelector.(selector.ExploreUnion)
	if isUnion {
		replacementMembers := make([]selector.Selector, 0, len(exploreUnion.Members))
		for _, sel := range exploreUnion.Members {
			newSelector := s.replaceRecursiveEdge(sel, replacement)
			if newSelector != nil {
				replacementMembers = append(replacementMembers, newSelector)
			}
		}
		if len(replacementMembers) == 0 {
			return nil
		}
		if len(replacementMembers) == 1 {
			return replacementMembers[0]
		}
		return selector.ExploreUnion{Members: replacementMembers}
	}
	return nextSelector
}

// Decide if a node directly matches
func (s ExploreRecursive) Decide(n datamodel.Node) bool {
	return s.current.Decide(n)
}

// Match always returns false because this is not a matcher
func (s ExploreRecursive) Match(node datamodel.Node) (datamodel.Node, error) {
	return s.current.Match(node)
}

type exploreRecursiveContext struct {
	path       string
	edgesFound int
}

func (erc *exploreRecursiveContext) Link(s selector.Selector) bool {
	_, ok := s.(selector.ExploreRecursiveEdge)
	if ok {
		//fmt.Println("found")
		erc.edgesFound++
	}
	return ok
}

// ParseExploreRecursive assembles a Selector from a ExploreRecursive selector node
func (er *ERContext) ParseExploreRecursive(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector spec parse rejected: selector body must be a map")
	}

	limitNode, err := n.LookupByString(selector.SelectorKey_Limit)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: limit field must be present in ExploreRecursive selector")
	}
	limit, err := parseLimit(limitNode)
	if err != nil {
		return nil, err
	}
	sequence, err := n.LookupByString(selector.SelectorKey_Sequence)
	if err != nil {
		return nil, fmt.Errorf("selector spec parse rejected: sequence field must be present in ExploreRecursive selector")
	}
	erc := &exploreRecursiveContext{}
	selector1, err := er.ePc.PushParent(erc).ParseSelector(sequence)
	if err != nil {
		return nil, err
	}
	if erc.edgesFound == 0 {
		return nil, fmt.Errorf("selector spec parse rejected: ExploreRecursive must have at least one ExploreRecursiveEdge")
	}
	er.generate(erc)
	var stopCondition *selector.Condition
	stop, err := n.LookupByString(selector.SelectorKey_StopAt)
	if err == nil {
		condition, err := er.ePc.selPc.ParseCondition(stop)
		if err != nil {
			return nil, err
		}
		stopCondition = &condition
	}
	return ExploreRecursive{selector1, selector1, limit, stopCondition}, nil
}

func parseLimit(n datamodel.Node) (RecursionLimit, error) {
	if n.Kind() != datamodel.Kind_Map {
		return RecursionLimit{}, fmt.Errorf("selector spec parse rejected: limit in ExploreRecursive is a keyed union and thus must be a map")
	}
	if n.Length() != 1 {
		return RecursionLimit{}, fmt.Errorf("selector spec parse rejected: limit in ExploreRecursive is a keyed union and thus must be a single-entry map")
	}
	kn, v, _ := n.MapIterator().Next()
	kstr, _ := kn.AsString()
	switch kstr {
	case selector.SelectorKey_LimitDepth:
		maxDepthValue, err := v.AsInt()
		if err != nil {
			return RecursionLimit{}, fmt.Errorf("selector spec parse rejected: limit field of type depth must be a number in ExploreRecursive selector")
		}
		return RecursionLimit{selector.RecursionLimit_Depth, maxDepthValue}, nil
	case selector.SelectorKey_LimitNone:
		return RecursionLimit{selector.RecursionLimit_None, 0}, nil
	default:
		return RecursionLimit{}, fmt.Errorf("selector spec parse rejected: %q is not a known member of the limit union in ExploreRecursive", kstr)
	}
}
