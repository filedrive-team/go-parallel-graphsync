package parseselector

import (
	"fmt"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type ExplorePath struct {
	Path      string
	Recursive bool
	IsUnixfs  bool
}

type ExplorePathContext interface {
	SetRecursive(recursive bool)
	NotSupport() bool
	Get() []ExplorePath
}

type exploreRecursivePathContext struct {
	path       string
	recursive  bool
	isUnixfs   bool
	notSupport bool
}

func (erc *exploreRecursivePathContext) SetRecursive(recursive bool) {
	erc.recursive = recursive
}

func (erc *exploreRecursivePathContext) NotSupport() bool {
	return erc.notSupport
}

func (erc *exploreRecursivePathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      erc.path,
		Recursive: erc.recursive,
		IsUnixfs:  erc.isUnixfs,
	},
	}
}

type exploreMatchPathContext struct {
	path       string
	recursive  bool
	isUnixfs   bool
	notSupport bool
}

func (emc *exploreMatchPathContext) SetRecursive(recursive bool) {
	emc.recursive = recursive
}

func (emc *exploreMatchPathContext) NotSupport() bool {
	return emc.notSupport
}

func (emc *exploreMatchPathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      emc.path,
		Recursive: emc.recursive,
		IsUnixfs:  emc.isUnixfs,
	},
	}
}

type exploreIndexPathContext struct {
	path       string
	index      int64
	recursive  bool
	notSupport bool
}

func (eic *exploreIndexPathContext) SetRecursive(recursive bool) {
	eic.recursive = recursive
}

func (eic *exploreIndexPathContext) NotSupport() bool {
	return eic.notSupport
}

func (eic *exploreIndexPathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      fmt.Sprintf("%s/%d/Hash", eic.path, eic.index),
		Recursive: eic.recursive,
	},
	}
}

type exploreRangePathContext struct {
	path       string
	start      int64
	end        int64
	recursive  bool
	notSupport bool
}

func (erc *exploreRangePathContext) SetRecursive(recursive bool) {
	erc.recursive = recursive
}

func (erc *exploreRangePathContext) NotSupport() bool {
	return erc.notSupport
}

func (erc *exploreRangePathContext) Get() []ExplorePath {
	paths := make([]ExplorePath, 0, erc.end-erc.start)
	for i := erc.start; i < erc.end; i++ {
		paths = append(paths, ExplorePath{
			Path:      fmt.Sprintf("%s/%d/Hash", erc.path, i),
			Recursive: erc.recursive,
		})
	}
	return paths
}

func checkNextSelector(sel selector.Selector) (recursive bool, notSupport bool) {
	switch sel.(type) {
	case ExploreRecursive:
		recursive = true
	case selector.Matcher:
		recursive = false
	case selector.ExploreAll:
		allSel := sel.(selector.ExploreAll)
		// get next selector
		if nextSel, err := allSel.Explore(nil, datamodel.PathSegment{}); err != nil {
			notSupport = true
		} else {
			switch nextSel.(type) {
			case selector.ExploreRecursiveEdge:
				recursive = true
			default:
				notSupport = true
			}
		}
	case selector.ExploreUnion:
		unionSel := sel.(selector.ExploreUnion)
		for _, subSel := range unionSel.Members {
			unionRecursive, unionNotSupport := checkNextSelector(subSel)
			recursive = recursive || unionRecursive
			notSupport = notSupport || unionNotSupport
			if notSupport {
				return
			}
		}
	case ExploreIndex:
		// more actions
		indexSel := sel.(ExploreIndex)
		indexRecursive, indexNotSupport := checkNextSelector(indexSel.next)
		recursive = recursive || indexRecursive
		notSupport = notSupport || indexNotSupport
		if notSupport {
			return
		}
	default:
		notSupport = true
	}
	return
}
