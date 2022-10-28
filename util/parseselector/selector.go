package parseselector

import (
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"strings"

	"github.com/ipld/go-ipld-prime/datamodel"
)

type ERParseContext struct {
	selPc       selector.ParseContext
	pathSegment []string
}

type ERContext struct {
	eCtx  []ExplorePathContext
	ePc   ERParseContext
	union []string
}

// ParseSelector creates a Selector from an IPLD Selector Node with the given context
//todo Maybe there is a logical problem, and the recursive call will transfer to the method of IPLD prime,
//todo but there is no problem trying to build a selector that meets the requirements, maybe it will be fixed in the future when problems arise
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
		return er.ParseExploreIndex(v)
	case selector.SelectorKey_ExploreRange:
		return er.ParseExploreRange(v)
	case selector.SelectorKey_ExploreUnion:
		er.union = er.ePc.pathSegment
		//fmt.Println("|||", er.pc.path)
		return er.ParseExploreUnion(v)
	case selector.SelectorKey_ExploreRecursive:
		return er.ParseExploreRecursive(v)
	case selector.SelectorKey_ExploreRecursiveEdge:
		return er.ePc.selPc.ParseExploreRecursiveEdge(v)
	case selector.SelectorKey_ExploreInterpretAs:
		return er.ParseExploreInterpretAs(v)
	case selector.SelectorKey_Matcher:
		return er.ParseMatcher(v)
	default:
		return nil, fmt.Errorf("selector spec parse rejected: %q is not a known member of the selector union", kstr)
	}
}

// PushParent puts a parent onto the stack of parents for a parse context
func (c *ERParseContext) PushParent(parent selector.ParsedParent) *ERContext {
	return &ERContext{ePc: ERParseContext{selPc: c.selPc.PushParent(parent)}}
}

// PushLinks puts a parent onto the stack of parents for a parse context
func (c *ERParseContext) PushLinks(l string) {
	c.pathSegment = append(c.pathSegment, l)
}

func (er *ERContext) collectPath(erc ExplorePathContext) {
	paths := erc.Get()
	if len(paths) > 0 {
		if strings.HasSuffix(paths[0].Path, "Hash") {
			er.eCtx = append(er.eCtx, erc)
		}
		if paths[0].IsUnixFS {
			er.eCtx = append(er.eCtx, erc)
		}
	}
}

func newPathFromPathSegments(paths []string) string {
	var ps []datamodel.PathSegment
	for _, p := range paths {
		ps = append(ps, datamodel.PathSegmentOfString(p))
	}
	return datamodel.NewPath(ps).String()
}

type ParsedSelectors struct {
	Path      string
	Sel       ipld.Node
	IsUnixFS  bool
	Recursive bool
}

func GenerateSelectors(sel ipld.Node) (parsedSelectors []ParsedSelectors, err error) {
	var er = &ERContext{}
	_, err = er.ParseSelector(sel)
	if err != nil {
		return nil, err
	}
	for _, ec := range er.eCtx {
		paths := ec.Get()
		for _, ep := range paths {
			var spec ipld.Node
			if ep.IsUnixFS {
				spec = util.UnixFSPathSelectorNotRecursive(ep.Path)
			} else {
				spe, _ := textselector.SelectorSpecFromPath(textselector.Expression(ep.Path), false, nil)
				spec = spe.Node()
			}

			parsedSelectors = append(parsedSelectors, ParsedSelectors{
				Path:      ep.Path,
				Sel:       spec,
				IsUnixFS:  ep.IsUnixFS,
				Recursive: ep.Recursive,
			})
		}
	}
	return parsedSelectors, nil
}

// ParseMatcher assembles a Selector
// from a matcher selector node
// TODO: Parse labels and conditions
func (er *ERContext) ParseMatcher(n datamodel.Node) (selector.Selector, error) {
	matcher, err := er.ePc.selPc.ParseMatcher(n)
	if err != nil || matcher == nil {
		return nil, err
	}
	er.collectPath(&exploreMatchPathContext{
		path: newPathFromPathSegments(er.ePc.pathSegment),
	})
	return matcher, nil
}

// ParseExploreUnion assembles a Selector
// from an ExploreUnion selector node
func (er *ERContext) ParseExploreUnion(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_List {
		return nil, fmt.Errorf("selector spec parse rejected: explore union selector must be a list")
	}
	x := selector.ExploreUnion{
		Members: make([]selector.Selector, 0, n.Length()),
	}
	pa := er.union
	for itr := n.ListIterator(); !itr.Done(); {
		_, v, err := itr.Next()
		if err != nil {
			return nil, fmt.Errorf("error during selector spec parse: %w", err)
		}
		member, err := er.ParseSelector(v)
		if err != nil {
			return nil, err
		}
		//clear store path
		er.ePc.pathSegment = nil
		er.ePc.pathSegment = append(er.ePc.pathSegment, pa...)
		x.Members = append(x.Members, member)
	}
	return x, nil
}
