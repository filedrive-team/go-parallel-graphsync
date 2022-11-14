package requestmanger

import (
	"context"
	"errors"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/gsrespserver"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/filedrive-team/go-parallel-graphsync/util/parseselector"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"math"
	"sync"
	"time"
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
	errorsChan            chan error
	pGServerManager       *gsrespserver.ParallelGraphServerManger
}

func NewParGraphSyncRequestManger(pgs pargraphsync.ParallelGraphExchange, root cidlink.Link, parallelGraphServers *gsrespserver.ParallelGraphServerManger) *ParallelGraphRequestManger {
	return &ParallelGraphRequestManger{
		parallelGraphExchange: pgs,
		collectedRequests:     make(map[string]struct{}),
		runningRequests:       make(map[string]struct{}),
		requestChan:           make(chan []pargraphsync.RequestParam, 1),
		doneRequests:          make(map[string]struct{}),
		rootCid:               root,
		errorsChan:            make(chan error),
		pGServerManager:       parallelGraphServers,
	}
}
func StartPraGraphSync(ctx context.Context, pgs pargraphsync.ParallelGraphExchange, selector ipld.Node, root cidlink.Link, parallelGraphServerManger *gsrespserver.ParallelGraphServerManger) error {
	selectors, err := parseselector.GenerateSelectors(selector)
	if err != nil {
		return err
	}
	s := NewParGraphSyncRequestManger(pgs, root, parallelGraphServerManger)
	s.RegisterCollectSpeedInfo(ctx)
	ctx, cancel := context.WithCancel(ctx)
	var completed = make(chan bool)
	go func() {
		defer cancel()
		cCids := s.collectSelectorSubTreeCid(ctx, selectors)
		for _, ci := range cCids {
			if ci.recursive {
				s.startParGraphSyncRequestManger(ctx, ci.cid)
			}
		}
		completed <- true
	}()
	select {
	case err = <-s.errorsChan:
		if err != nil {
			fmt.Println(err)
			return err
		}
	case <-completed:
		return nil
	case <-ctx.Done():
		return errors.New("context cancel")
	}
	return nil
}

type collectCid struct {
	cid       cidlink.Link
	recursive bool
}

func (s *ParallelGraphRequestManger) collectSelectorSubTreeCid(ctx context.Context, selectors []parseselector.ParsedSelectors) []collectCid {
	var cids []collectCid
	var nilCollectCid collectCid
	for _, ne := range selectors {
		cCid := s.collectResponses(ctx, ne)
		if cCid != nilCollectCid {
			cids = append(cids, cCid)
		} else {
			return nil
		}
	}
	return cids
}
func (s *ParallelGraphRequestManger) collectResponses(ctx context.Context, ne parseselector.ParsedSelectors) collectCid {
	responseProgress, errorChan := s.parallelGraphExchange.Request(ctx, s.pGServerManager.GetIdlePeer(ctx), s.rootCid, ne.Sel)
	for {
		select {
		case err := <-errorChan:
			if err != nil {
				s.errorsChan <- err
				return collectCid{}
			}
		case blk := <-responseProgress:
			if ne.Path == blk.Path.String() {
				fmt.Printf("edge:%v path=%s:%s \n", ne.Recursive, blk.Path.String(), blk.LastBlock.Link.String())
				ci, _ := cid.Parse(blk.LastBlock.Link.String())
				// check if blk have sub node,Need to check? Or is this correct to check?
				re := ne.Recursive
				if nd, err := blk.Node.LookupByString("Links"); err != nil || nd.Length() == 0 {
					re = false
				}
				return collectCid{
					cid:       cidlink.Link{Cid: ci},
					recursive: re,
				}
			}
		}
	}
}

// initParGraphSyncRequestManger
func (s *ParallelGraphRequestManger) startParGraphSyncRequestManger(ctx context.Context, root cidlink.Link) {
	s.requestChan <- []pargraphsync.RequestParam{{PeerId: s.pGServerManager.GetIdlePeer(ctx), Root: root, Selector: util.LeftSelector("")}}
	s.StartRun(ctx)
}

// StartRun you can also build a ParallelGraphRequestManger yourself and use this method to synchronize
func (s *ParallelGraphRequestManger) StartRun(ctx context.Context) {
	for {
		select {
		case request := <-s.requestChan:
			s.run(ctx, request)
		default:
			if !s.divideRequests(ctx) {
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
	responseProgress, errorsChan := s.parallelGraphExchange.RequestMany(ctx, params)
	go func() {
		select {
		case err := <-errorsChan:
			if err != nil {
				s.errorsChan <- err
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
	s.pGServerManager.FreeDealCount(ctx, params)
	s.collectRequests(pathMap)
}

func (s *ParallelGraphRequestManger) dividePaths(ctx context.Context, paths []string) []pargraphsync.RequestParam {
	ave := Ceil(len(paths), s.pGServerManager.GetPeerCount(ctx))
	var start, end = 0, 0
	num := s.pGServerManager.GetPeerCount(ctx)
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
			s.errorsChan <- err
		}
		requests = append(requests, pargraphsync.RequestParam{
			Selector: sel,
			Root:     s.rootCid,
			PeerId:   s.pGServerManager.GetIdlePeer(ctx),
		})
		start = end
	}
	return requests
}
func Ceil(x, y int) int {
	return int(math.Ceil(float64(x) / float64(y)))
}

func (s *ParallelGraphRequestManger) RegisterCollectSpeedInfo(ctx context.Context) {
	type co struct {
		lock  sync.Mutex
		start int64
		end   int64
		cost  int64
		size  uint64
	}
	ti := make(map[string]*co, 1)
	s.parallelGraphExchange.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		if _, ok := ti[p.String()+request.ID().String()]; !ok {
			ti[p.String()+request.ID().String()] = &co{start: time.Now().UnixNano()}
		}
	})
	s.parallelGraphExchange.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		id := p.String() + responseData.RequestID().String()
		ti[id].lock.Lock()
		defer ti[id].lock.Unlock()
		ti[id].cost = time.Now().UnixNano() - ti[id].start
		ti[id].size += blockData.BlockSize()
		//todo:maybe more efficient
		s.pGServerManager.UpdateSpeed(ctx, p.String(), calculateSpeed(ti[id].size, ti[id].cost))
	})
}

func calculateSpeed(x uint64, y int64) uint64 {
	// byte/(ns/1000000)/1024=kb/ms
	// todo more efficient calculation
	return uint64(math.Ceil(float64(x) / (float64(y) / 1_000_000.0) / 1024))
}
