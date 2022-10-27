package util

import (
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/libp2p/go-libp2p-core/peer"
	"math"
)

const (
	LeafLinksTemplate  = "/%v/Hash/Links"
	LeftLinksTemplate  = "Links/0/Hash"
	CheckLinksTemplate = "/%v/Hash"
)

type ParallelGraphRequestManger struct {
	parallelGraphExchange pargraphsync.ParallelGraphExchange
	collectedRequests     map[string]struct{}
	runningRequests       map[string]struct{}
	requestChan           chan []pargraphsync.RequestParam
	doneRequests          map[string]struct{}
	rootCid               cidlink.Link
	peerInfos             []peer.AddrInfo
}

//todo implement choose
func (s *ParallelGraphRequestManger) choosePeer() peer.ID {
	return s.peerInfos[0].ID
}
func StartParGraphSyncRequestManger(ctx context.Context, pgs pargraphsync.ParallelGraphExchange, root cidlink.Link, peerIds []peer.AddrInfo) {
	var s = ParallelGraphRequestManger{
		parallelGraphExchange: pgs,
		collectedRequests:     make(map[string]struct{}),
		runningRequests:       make(map[string]struct{}),
		requestChan:           make(chan []pargraphsync.RequestParam, 1),
		doneRequests:          make(map[string]struct{}),
		rootCid:               root,
		peerInfos:             peerIds,
	}
	s.requestChan <- []pargraphsync.RequestParam{{PeerId: s.choosePeer(), Root: root, Selector: LeftSelector("")}}
	s.StartRun(ctx)
}
func (s *ParallelGraphRequestManger) StartRun(ctx context.Context) {
	for {
		select {
		case request := <-s.requestChan:
			s.run(ctx, request)
		default:
			if !s.divideRequests() {
				s.Close()
				return
			}
		}
	}
}

func (s *ParallelGraphRequestManger) Close() {
	//todo more action
	fmt.Println("close")
}
func (s *ParallelGraphRequestManger) collectRequests(pathMap map[string]int64) {
	for k, v := range pathMap {
		for i := int64(0); i < v; i++ {
			if s.checkRequests(k + fmt.Sprintf(CheckLinksTemplate, i)) {
				continue
			}
			s.collectedRequests[k+fmt.Sprintf(LeafLinksTemplate, i)] = struct{}{}
		}
	}
}

func (s *ParallelGraphRequestManger) divideRequests() bool {
	var paths []string
	for k, v := range s.collectedRequests {
		s.runningRequests[k] = v
		paths = append(paths, k)
		delete(s.collectedRequests, k)
	}
	if len(paths) == 0 {
		return false
	}
	s.requestChan <- s.dividePaths(paths)
	return true
}

func (s *ParallelGraphRequestManger) checkRequests(path string) bool {

	if _, ok := s.collectedRequests[path]; ok {
		return true
	}
	if _, ok := s.runningRequests[path]; ok {
		return true
	}
	if _, ok := s.doneRequests[path]; ok {
		return true
	}
	return false
}

func (s *ParallelGraphRequestManger) run(ctx context.Context, params []pargraphsync.RequestParam) {
	responseProgress, errors := s.parallelGraphExchange.RequestMany(ctx, params)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				return
			}
		}
	}()
	pathMap := make(map[string]int64)
	for blk := range responseProgress {
		s.doneRequests[blk.Path.String()] = struct{}{}
		//fmt.Printf("path:%v\n", blk.Path)
		if nd, err := blk.Node.LookupByString("Links"); err == nil && nd.Length() != 0 {
			//fmt.Printf("links=%d\n", nd.Length())
			if blk.Path.String() == "" {
				pathMap[blk.Path.String()+"Links"] = nd.Length()
			} else {
				pathMap[blk.Path.String()+"/Links"] = nd.Length()
			}
		}
	}
	s.collectRequests(pathMap)
}

func (s *ParallelGraphRequestManger) dividePaths(paths []string) []pargraphsync.RequestParam {
	ave := int(math.Ceil(float64(len(paths)) / float64(len(s.peerInfos))))
	var start, end = 0, 0
	num := len(s.peerInfos)
	if num > len(paths) {
		num = len(paths)
	}
	var requests []pargraphsync.RequestParam
	for i := 0; i < num; i++ {
		end = start + ave
		if i == num-1 {
			end = len(paths)
		}
		sel, err := UnionPathSelector(paths[start:end], true)
		// continue or return
		if err != nil {
			continue
		}
		// todo should choose idle peer
		requests = append(requests, pargraphsync.RequestParam{
			Selector: sel,
			Root:     s.rootCid,
			PeerId:   s.peerInfos[i].ID,
		})
		start = end
	}
	return requests
}
func LeftSelector(path string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selSpec, _ := textselector.SelectorSpecFromPath(LeftLinksTemplate, false, ssb.ExploreRecursiveEdge())
	fromPath, _ := textselector.SelectorSpecFromPath(textselector.Expression(path), false, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
	return fromPath.Node()
}
