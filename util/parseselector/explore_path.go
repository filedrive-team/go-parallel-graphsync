package parseselector

import (
	"fmt"
)

type ExplorePath struct {
	Path      string
	Recursive bool
	IsUnixFS  bool
}

type ExplorePathContext interface {
	NotSupport() bool
	Get() []ExplorePath
}

type exploreRecursivePathContext struct {
	path       string
	notSupport bool
}

func (erc *exploreRecursivePathContext) NotSupport() bool {
	return erc.notSupport
}

func (erc *exploreRecursivePathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      erc.path,
		Recursive: true,
		IsUnixFS:  false,
	},
	}
}

type exploreMatchPathContext struct {
	path       string
	notSupport bool
}

func (emc *exploreMatchPathContext) NotSupport() bool {
	return emc.notSupport
}

func (emc *exploreMatchPathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      emc.path,
		Recursive: false,
		IsUnixFS:  false,
	},
	}
}

type exploreIndexPathContext struct {
	path       string
	index      int64
	recursive  bool
	notSupport bool
}

func (eic *exploreIndexPathContext) NotSupport() bool {
	return eic.notSupport
}

func (eic *exploreIndexPathContext) Get() []ExplorePath {
	return []ExplorePath{{
		Path:      fmt.Sprintf("%s/%d/Hash", eic.path, eic.index),
		Recursive: eic.recursive,
		IsUnixFS:  false,
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

type exploreUnixFSPathContext struct {
	path       string
	recursive  bool
	notSupport bool
}

func (erc *exploreUnixFSPathContext) NotSupport() bool {
	return erc.notSupport
}

func (erc *exploreUnixFSPathContext) Get() []ExplorePath {

	return []ExplorePath{
		{
			Path:      erc.path,
			Recursive: erc.recursive,
			IsUnixFS:  true,
		},
	}
}
