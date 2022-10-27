package parseselector

import (
	"fmt"
)

type ExplorePath struct {
	Path      string
	Recursive bool
}

type ExplorePathContext interface {
	Get() []ExplorePath
}

type exploreRecursivePathContext struct {
	path      string
	recursive bool
}

func (erc *exploreRecursivePathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      erc.path,
		Recursive: erc.recursive,
	},
	}
}

type exploreIndexPathContext struct {
	path      string
	index     int64
	recursive bool
}

func (eic *exploreIndexPathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      fmt.Sprintf("%s/%d/Hash", eic.path, eic.index),
		Recursive: eic.recursive,
	},
	}
}

type exploreRangePathContext struct {
	path      string
	start     int64
	end       int64
	recursive bool
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
