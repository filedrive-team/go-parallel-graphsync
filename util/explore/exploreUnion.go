package explore

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/traversal/selector"

	"github.com/ipld/go-ipld-prime/datamodel"
)

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
		er.ePc.path = nil
		er.ePc.path = append(er.ePc.path, pa...)
		x.Members = append(x.Members, member)
	}
	return x, nil
}
