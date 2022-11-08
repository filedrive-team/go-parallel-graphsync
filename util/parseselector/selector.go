package parseselector

import (
	"errors"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"strings"
)

type ERParseContext struct {
	origin              selector.ParseContext
	pathSegments        []string
	explorePathContexts []ExplorePathContext
	isUnixfs            bool
}

type ParsedSelectors struct {
	Path      string
	Sel       ipld.Node
	IsUnixFS  bool
	Recursive bool
}

// ParseSelector creates a Selector from an IPLD Selector Node with the given context
//todo Maybe there is a logical problem, and the recursive call will transfer to the method of IPLD prime,
//todo but there is no problem trying to build a selector that meets the requirements, maybe it will be fixed in the future when problems arise
func (er *ERParseContext) ParseSelector(n datamodel.Node) (selector.Selector, error) {
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
		return er.origin.ParseExploreAll(v)
	case selector.SelectorKey_ExploreIndex:
		return er.ParseExploreIndex(v)
	case selector.SelectorKey_ExploreRange:
		return er.ParseExploreRange(v)
	case selector.SelectorKey_ExploreUnion:
		return er.ParseExploreUnion(v)
	case selector.SelectorKey_ExploreRecursive:
		return er.ParseExploreRecursive(v)
	case selector.SelectorKey_ExploreRecursiveEdge:
		return er.origin.ParseExploreRecursiveEdge(v)
	case selector.SelectorKey_ExploreInterpretAs:
		return er.ParseExploreInterpretAs(v)
	case selector.SelectorKey_Matcher:
		return er.ParseMatcher(v)
	default:
		return nil, fmt.Errorf("selector spec parse rejected: %q is not a known member of the selector union", kstr)
	}
}

// PushParent puts a parent onto the stack of parents for a parse context
func (er *ERParseContext) PushParent(parent selector.ParsedParent) *ERParseContext {
	return &ERParseContext{origin: er.origin.PushParent(parent)}
}

// PushPathSegment puts a path segment onto the stack of parents for a parse context
func (er *ERParseContext) PushPathSegment(segment string) {
	er.pathSegments = append(er.pathSegments, segment)
}

func (er *ERParseContext) collectPath(erc ExplorePathContext) {
	paths := erc.Get()
	if len(paths) > 0 {
		if erc.NotSupport() {
			er.explorePathContexts = append(er.explorePathContexts, erc)
		} else {
			if er.isUnixfs {
				erc.SetRecursive(true)
				er.explorePathContexts = append(er.explorePathContexts, erc)
			} else if strings.HasSuffix(paths[0].Path, "Hash") {
				er.explorePathContexts = append(er.explorePathContexts, erc)
			} else {
				fmt.Println(erc.Get())
			}
		}
	}
}

// ParseMatcher assembles a Selector
// from a matcher selector node
// TODO: Parse labels and conditions
func (er *ERParseContext) ParseMatcher(n datamodel.Node) (selector.Selector, error) {
	matcher, err := er.origin.ParseMatcher(n)
	if err != nil || matcher == nil {
		return nil, err
	}
	er.collectPath(&exploreMatchPathContext{
		path:     newPathFromPathSegments(er.pathSegments),
		isUnixfs: er.isUnixfs,
	})
	return matcher, nil
}

// ParseExploreUnion assembles a Selector
// from an ExploreUnion selector node
func (er *ERParseContext) ParseExploreUnion(n datamodel.Node) (selector.Selector, error) {
	if n.Kind() != datamodel.Kind_List {
		return nil, fmt.Errorf("selector spec parse rejected: explore union selector must be a list")
	}
	x := selector.ExploreUnion{
		Members: make([]selector.Selector, 0, n.Length()),
	}
	top := er.pathSegments[:]
	for itr := n.ListIterator(); !itr.Done(); {
		_, v, err := itr.Next()
		if err != nil {
			return nil, fmt.Errorf("error during selector spec parse: %w", err)
		}
		member, err := er.ParseSelector(v)
		if err != nil {
			return nil, err
		}
		// restore path
		er.pathSegments = top
		x.Members = append(x.Members, member)
	}
	return x, nil
}

func newPathFromPathSegments(paths []string) string {
	var ps []datamodel.PathSegment
	for _, p := range paths {
		ps = append(ps, datamodel.PathSegmentOfString(p))
	}
	return datamodel.NewPath(ps).String()
}

func GenerateSelectors(sel ipld.Node) (selectors []ParsedSelectors, err error) {
	er := &ERParseContext{}
	_, err = er.ParseSelector(sel)
	if err != nil {
		return nil, err
	}
	for _, ec := range er.explorePathContexts {
		if ec.NotSupport() {
			return nil, errors.New("not support")
		}
		paths := ec.Get()
		for _, ep := range paths {
			var spec builder.SelectorSpec
			if ep.IsUnixfs {
				spec = util.UnixFSPathSelectorNotRecursive(ep.Path)
			} else {
				spec, _ = textselector.SelectorSpecFromPath(textselector.Expression(ep.Path), false, nil)
			}
			selectors = append(selectors, ParsedSelectors{
				Path:      ep.Path,
				Sel:       spec.Node(),
				IsUnixFS:  ep.IsUnixfs,
				Recursive: ep.Recursive,
			})
		}
	}
	return selectors, nil
}
