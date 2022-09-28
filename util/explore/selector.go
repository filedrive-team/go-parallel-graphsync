package explore

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"strings"

	"github.com/ipld/go-ipld-prime/datamodel"
)

type ERParseContext struct {
	selPc selector.ParseContext
	path  []string
}
type ERContext struct {
	eCtx  []*exploreRecursiveContext
	ePc   ERParseContext
	union []string
}

// ParseSelector creates a Selector from an IPLD Selector Node with the given context
func (er *ERContext) ParseSelector(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("selector spec parse rejected: selector is a keyed union and thus must be a map")
	}
	if n.Length() != 1 {
		return nil, fmt.Errorf("selector spec parse rejected: selector is a keyed union and thus must be single-entry map")
	}

	kn, v, _ := n.MapIterator().Next()
	kstr, _ := kn.AsString()
	// Switch over the single key to determine which selector body comes next.
	//  (This switch is where the keyed union discriminators concretely happen.)
	switch kstr {
	case selector.SelectorKey_ExploreFields:
		return er.ParseExploreFields(v)
	case selector.SelectorKey_ExploreAll:
		return er.ePc.selPc.ParseExploreAll(v)
	case selector.SelectorKey_ExploreIndex:
		return er.ePc.selPc.ParseExploreIndex(v)
	case selector.SelectorKey_ExploreRange:
		return er.ePc.selPc.ParseExploreRange(v)
	case selector.SelectorKey_ExploreUnion:
		er.union = er.ePc.path
		//fmt.Println("|||", er.pc.path)
		return er.ParseExploreUnion(v)
	case selector.SelectorKey_ExploreRecursive:
		return er.ParseExploreRecursive(v)
	case selector.SelectorKey_ExploreRecursiveEdge:
		return er.ePc.selPc.ParseExploreRecursiveEdge(v)
	case selector.SelectorKey_ExploreInterpretAs:
		return er.ePc.selPc.ParseExploreInterpretAs(v)
	case selector.SelectorKey_Matcher:
		return er.ParseMatcher(v)
	default:
		return nil, fmt.Errorf("selector spec parse rejected: %q is not a known member of the selector union", kstr)
	}
}

// PushParent puts a parent onto the stack of parents for a parse context
func (c ERParseContext) PushParent(parent selector.ParsedParent) *ERContext {
	return &ERContext{ePc: ERParseContext{selPc: c.selPc.PushParent(parent)}}
}

// PushLinks puts a parent onto the stack of parents for a parse context
func (c *ERParseContext) PushLinks(l string) {
	c.path = append(c.path, l)
}
func (er *ERContext) generate(erc *exploreRecursiveContext) {
	var ssss []datamodel.PathSegment
	for _, p := range er.ePc.path {
		ssss = append(ssss, datamodel.PathSegmentOfString(p))
	}
	erc.path = datamodel.NewPath(ssss).String()
	//ectx = append(ectx, erc)
	if strings.HasSuffix(erc.path, "Hash") {
		er.eCtx = append(er.eCtx, erc)
	}

}
