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
	"sync"
)

const (
	LeafLinksTemplate  = "/%v/Hash/Links"
	LeftLinks          = "Links/0/Hash"
	CheckLinksTemplate = "/%v/Hash"
)

type ParallelGraphRequestManger struct {
	parallelGraphExchange pargraphsync.ParallelGraphExchange
	collectedRequests     map[string]struct{}
	runningRequests       map[string]struct{}
	requestChan           chan []pargraphsync.RequestParam
	doneRequests          map[string]struct{}
	rootCid               cidlink.Link
	parallelGraphServers  map[string]*ParallelGraphServer
}

type ParallelGraphServer struct {
	lock      sync.Mutex
	dealCount int
	//todo more about server info
	addrInfo peer.AddrInfo
}

func (s *ParallelGraphRequestManger) ChoosePeer() peer.ID {
	//todo implement more about choose eg:latency and transfer speed
	var dealCount = 1000
	var peerId peer.ID
	var pgs *ParallelGraphServer
	// todo check latency and transfer speed
	for _, pgDServer := range s.parallelGraphServers {
		if pgDServer.dealCount < dealCount {
			dealCount = pgDServer.dealCount
			peerId = pgDServer.addrInfo.ID
			pgDServer.dealCount++
			pgs = pgDServer
		}
	}
	s.parallelGraphServers[peerId.String()].lock.Lock()
	s.parallelGraphServers[peerId.String()] = pgs
	s.parallelGraphServers[peerId.String()].lock.Unlock()
	return peerId
}

// StartParGraphSyncRequestManger
// this func will sync all subNode of the rootNode
// you can use the util/parseselector/GenerateSelectors() to parse a complex selector ,then use this func to sync
func StartParGraphSyncRequestManger(ctx context.Context, pgs pargraphsync.ParallelGraphExchange, root cidlink.Link, infos []peer.AddrInfo) {
	var s = ParallelGraphRequestManger{
		parallelGraphExchange: pgs,
		collectedRequests:     make(map[string]struct{}),
		runningRequests:       make(map[string]struct{}),
		requestChan:           make(chan []pargraphsync.RequestParam, 1),
		doneRequests:          make(map[string]struct{}),
		rootCid:               root,
		parallelGraphServers:  addrInfoToParallelGraphServers(infos),
	}
	s.requestChan <- []pargraphsync.RequestParam{{PeerId: s.ChoosePeer(), Root: root, Selector: LeftSelector("")}}
	s.StartRun(ctx)
}
func addrInfoToParallelGraphServers(infos []peer.AddrInfo) map[string]*ParallelGraphServer {
	parallelGraphServers := make(map[string]*ParallelGraphServer)
	for _, info := range infos {
		parallelGraphServers[info.ID.String()] = &ParallelGraphServer{
			dealCount: 0,
			addrInfo:  info,
		}
	}
	return parallelGraphServers
}

// StartRun you can also build a ParallelGraphRequestManger yourself and use this method to synchronize
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

// Close the Manger
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
	s.freeParallelGraphServer(params)
	s.collectRequests(pathMap)
}
func (s *ParallelGraphRequestManger) freeParallelGraphServer(params []pargraphsync.RequestParam) {
	for _, param := range params {
		s.parallelGraphServers[param.PeerId.String()].lock.Lock()
		s.parallelGraphServers[param.PeerId.String()].dealCount--
		s.parallelGraphServers[param.PeerId.String()].lock.Unlock()
	}
}

func (s *ParallelGraphRequestManger) dividePaths(paths []string) []pargraphsync.RequestParam {
	ave := int(math.Ceil(float64(len(paths)) / float64(len(s.parallelGraphServers))))
	var start, end = 0, 0
	num := len(s.parallelGraphServers)
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
		requests = append(requests, pargraphsync.RequestParam{
			Selector: sel,
			Root:     s.rootCid,
			PeerId:   s.ChoosePeer(),
		})
		start = end
	}
	return requests
}
func LeftSelector(path string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selSpec, _ := textselector.SelectorSpecFromPath(LeftLinks, false, ssb.ExploreRecursiveEdge())
	fromPath, _ := textselector.SelectorSpecFromPath(textselector.Expression(path), false, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
	return fromPath.Node()
}
