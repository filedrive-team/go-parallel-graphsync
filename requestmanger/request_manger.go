package requestmanger

import (
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/gsrespserver"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"math"
)

const (
	LeafLinksTemplate  = "/%v/Hash/Links"
	CheckLinksTemplate = "/%v/Hash"
)

type ParallelGraphRequestManger struct {
	parallelGraphExchange pargraphsync.ParallelGraphExchange
	collectedRequests     map[string]struct{}
	runningRequests       map[string]struct{}
	requestChan           chan []pargraphsync.RequestParam
	doneRequests          map[string]struct{}
	rootCid               cidlink.Link
	errors                chan error
	parallelGraphServers  *gsrespserver.ParallelGraphServerManger
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
		parallelGraphServers:  gsrespserver.NewParallelGraphServerManger(infos),
	}
	s.requestChan <- []pargraphsync.RequestParam{{PeerId: s.parallelGraphServers.GetIdlePeer(ctx), Root: root, Selector: util.LeftSelector("")}}
	s.StartRun(ctx)
}

// StartRun you can also build a ParallelGraphRequestManger yourself and use this method to synchronize
func (s *ParallelGraphRequestManger) StartRun(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	for {
		select {
		case request := <-s.requestChan:
			s.run(ctx, request)
		case err := <-s.errors:
			if err != nil {
				fmt.Printf("request error: %v\n", err)
				cancel()
			}
		default:
			if !s.divideRequests(ctx) {
				s.Close(cancel)
				return
			}
		}
	}
}

// Close the Manger
func (s *ParallelGraphRequestManger) Close(cancel context.CancelFunc) {
	//todo more action
	cancel()
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

func (s *ParallelGraphRequestManger) divideRequests(ctx context.Context) bool {
	var paths []string
	for k, v := range s.collectedRequests {
		s.runningRequests[k] = v
		paths = append(paths, k)
		delete(s.collectedRequests, k)
	}
	if len(paths) == 0 {
		return false
	}
	s.requestChan <- s.dividePaths(ctx, paths)
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
	s.parallelGraphServers.FreeDealCount(ctx, params)
	s.collectRequests(pathMap)
}

func (s *ParallelGraphRequestManger) dividePaths(ctx context.Context, paths []string) []pargraphsync.RequestParam {
	ave := Ceil(len(paths), s.parallelGraphServers.GetPeerCount(ctx))
	var start, end = 0, 0
	num := s.parallelGraphServers.GetPeerCount(ctx)
	if num > len(paths) {
		num = len(paths)
	}
	var requests []pargraphsync.RequestParam
	for i := 0; i < num; i++ {
		end = start + ave
		if i == num-1 {
			end = len(paths)
		}
		sel, err := util.UnionPathSelector(paths[start:end], true)
		// continue or return
		if err != nil {
			s.errors <- err
		}
		requests = append(requests, pargraphsync.RequestParam{
			Selector: sel,
			Root:     s.rootCid,
			PeerId:   s.parallelGraphServers.GetIdlePeer(ctx),
		})
		start = end
	}
	return requests
}
func Ceil(x, y int) int {
	return int(math.Ceil(float64(x) / float64(y)))
}
