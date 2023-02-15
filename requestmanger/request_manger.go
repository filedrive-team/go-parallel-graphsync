package requestmanger

import (
	"context"
	"crypto/sha256"
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
	"strings"
	"sync"
	"time"
)

type subRequest struct {
	Root       ipld.Link
	Selector   ipld.Node
	Path       string
	Extensions []graphsync.ExtensionData
}

type ParallelRequestManger struct {
	requestChan chan []subRequest
	// status: requesting, completed
	inProgressReq sync.Map // key: sha256(root+selector) value: requestId

	exchange   pargraphsync.ParallelGraphExchange
	pgManager  *gsrespserver.PeersGroupManager
	rootCid    ipld.Link
	selector   ipld.Node
	extensions []graphsync.ExtensionData

	returnedResponses chan graphsync.ResponseProgress
	returnedErrors    chan error
}

func NewParGraphSyncRequestManger(exchange pargraphsync.ParallelGraphExchange, manager *gsrespserver.PeersGroupManager,
	root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) *ParallelRequestManger {
	if manager.GetPeerCount() == 0 {
		panic("have no peer")
	}
	return &ParallelRequestManger{
		requestChan:       make(chan []subRequest, 1024),
		exchange:          exchange,
		rootCid:           root,
		selector:          selector,
		extensions:        extensions,
		pgManager:         manager,
		returnedResponses: make(chan graphsync.ResponseProgress),
		returnedErrors:    make(chan error),
	}
}

type jobCid struct {
	cid       ipld.Link
	recursive bool
}

func (m *ParallelRequestManger) singleErrorResponse(err error) (chan graphsync.ResponseProgress, chan error) {
	ch := make(chan graphsync.ResponseProgress)
	close(ch)
	errCh := make(chan error, 1)
	errCh <- err
	close(errCh)
	return ch, errCh
}

func (m *ParallelRequestManger) Start(ctx context.Context) (<-chan graphsync.ResponseProgress, <-chan error) {
	isAllSel := false
	selectors, err := parseselector.GenerateSelectors(m.selector)
	if err != nil {
		// if error is not support, then call origin Request method
		if err != parseselector.NotSupportError {
			return m.singleErrorResponse(err)
		}

		if util.IsAllSelector(m.selector) {
			isAllSel = true
		} else {
			peerId := m.pgManager.GetIdlePeers(1)
			if len(peerId) == 0 {
				return m.singleErrorResponse(errors.New("no idle peer"))
			}
			return m.exchange.Request(ctx, peerId[0], m.rootCid, m.selector)
		}
	}

	m.exchange.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		fmt.Printf("NetworkErrorListener peer=%s request requestId=%s error=%v\n", p.String(), request.ID().String(), err)
		// retry request
		m.pushSubRequest(ctx, []subRequest{{
			Root:       cidlink.Link{Cid: request.Root()},
			Selector:   request.Selector(),
			Extensions: m.extensions,
		},
		})
		m.pgManager.ReleasePeer(p)
	})

	m.RegisterCollectSpeedInfo(ctx)
	go func() {
		defer m.Close()

		// collect subtree root cid
		jobCidQueue := make(chan jobCid)
		go func() {
			defer close(jobCidQueue)
			if isAllSel {
				jobCidQueue <- jobCid{
					cid:       m.rootCid,
					recursive: true,
				}
				return
			}
			subtreeSels := make(chan parseselector.ParsedSelectors, len(selectors))
			defer close(subtreeSels)
			for _, sel := range selectors {
				subtreeSels <- sel
			}

			for {
				select {
				case sel := <-subtreeSels:
					if !m.collectSubtreeRoots(ctx, sel, jobCidQueue) {
						subtreeSels <- sel
					}
					if len(subtreeSels) == 0 {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		go func() {
			for job := range jobCidQueue {
				if job.recursive {
					select {
					case m.requestChan <- []subRequest{{
						Root:       job.cid,
						Selector:   util.LeftSelector(""),
						Extensions: m.extensions,
					}}:
					case <-ctx.Done():
						return
					}

				}
			}
		}()

		m.handleRequest(ctx)
	}()
	return m.returnedResponses, m.returnedErrors
}

func (m *ParallelRequestManger) collectSubtreeRoots(ctx context.Context, sel parseselector.ParsedSelectors, queue chan<- jobCid) bool {
	peerIds := m.pgManager.WaitIdlePeers(ctx, 1)
	if len(peerIds) == 0 {
		return false
	}
	defer m.pgManager.ReleasePeer(peerIds[0])
	success := true
	responseProgress, errorChan := m.exchange.Request(ctx, peerIds[0], m.rootCid, sel.Sel)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range errorChan {
			// TODO: cancel this group request
			m.returnedErrors <- fmt.Errorf("%v, try again later", e)
			success = false
		}
	}()
	for blk := range responseProgress {
		m.returnedResponses <- blk

		if sel.Path == blk.Path.String() {
			fmt.Printf("edge:%v path=%s \n", sel.Recursive, blk.Path.String())
			if blk.LastBlock.Link != nil {
				ci, _ := cid.Parse(blk.LastBlock.Link.String())
				// check if blk have sub node,Need to check? Or is this correct to check?
				re := sel.Recursive
				if nd, err := blk.Node.LookupByString("Links"); err != nil || nd.Length() == 0 {
					re = false
				}
				queue <- jobCid{
					cid:       cidlink.Link{Cid: ci},
					recursive: re,
				}
			}
		}
	}
	wg.Wait()
	return success
}

func (m *ParallelRequestManger) handleRequest(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()
	for {
		select {
		case request := <-m.requestChan:
			// get the idle peers
			peers := m.pgManager.WaitIdlePeers(ctx, len(request))
			if len(peers) == 0 {
				return
			}
			// TODO: merge multiple existing requests based on the number of idle peers
			// len(m.requestChan)
			for i, p := range peers {
				go func(index int, id peer.ID) {
					defer func() {
						fmt.Println("ReleasePeer: ", id.String())
						m.pgManager.ReleasePeer(id)
					}()
					m.syncData(ctx, id, request[index])
				}(i, p)
			}
			remain := request[len(peers):]
			if len(remain) > 0 {
				m.pushSubRequest(ctx, remain)
			}
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(m.requestChan) == 0 && m.pgManager.IsAllIdle() {
				return
			}
		}
	}
}

func (m *ParallelRequestManger) pushSubRequest(ctx context.Context, reqs []subRequest) {
	go func() {
		select {
		case m.requestChan <- reqs:
		case <-ctx.Done():
			return
		}
	}()
}

func generateKey(root ipld.Link, sel ipld.Node) string {
	var s strings.Builder
	s.WriteString(root.String())
	s.WriteString(util.SelectorToJson(sel))
	h := sha256.New()
	h.Write([]byte(s.String()))
	return string(h.Sum(nil))
}

func (m *ParallelRequestManger) syncData(ctx context.Context, p peer.ID, request subRequest) {
	fmt.Println("req selector: ", util.SelectorToJson(request.Selector))
	defer func() {
		fmt.Println("finish req selector: ", util.SelectorToJson(request.Selector))
	}()
	responseProgress, errorsChan := m.exchange.Request(ctx, p, request.Root, request.Selector, request.Extensions...)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range errorsChan {
			// TODO: cancel this group request or retry
			m.returnedErrors <- e
		}
	}()

	for blk := range responseProgress {
		m.returnedResponses <- blk

		if nd, err := blk.Node.LookupByString("Links"); err == nil && nd.Length() != 0 {
			if nd.Length() > 1 {
				path := blk.Path.String()
				links := nd.Length() - 1
				peers := int64(m.pgManager.GetPeerCount())
				requests := make([]subRequest, 0, peers)
				usedLinks := int64(0)
				for index := int64(0); index < peers; index++ {
					remainLinks := links - usedLinks
					remainPeers := peers - index
					avg := remainLinks / remainPeers
					if remainLinks%remainPeers != 0 {
						avg += 1
					}

					start := 1 + usedLinks
					end := start + avg
					usedLinks += avg
					if sel, err := util.GenerateSubRangeSelector(path, start, end); err != nil {
						// TODO: cancel this group request
						m.returnedErrors <- err
					} else {
						if _, loaded := m.inProgressReq.LoadOrStore(generateKey(request.Root, sel), 0); !loaded {
							subPath := ""
							// fill in the path field when there is only one ipld node
							if links == 1 {
								if path == "" {
									subPath = "Links"
								} else {
									subPath = path + "/Links"
								}
								subPath = subPath + "/1/Hash/Links"

							}
							requests = append(requests, subRequest{
								Root:       request.Root,
								Selector:   sel,
								Path:       subPath,
								Extensions: request.Extensions,
							})
						}
					}
				}
				if len(requests) > 0 {
					m.pushSubRequest(ctx, requests)
				}
			}
		}
	}
	wg.Wait()
}

// Close the Manger
func (m *ParallelRequestManger) Close() {
	close(m.returnedErrors)
	close(m.returnedResponses)
	close(m.requestChan)
}

func (m *ParallelRequestManger) dividePaths(ctx context.Context, paths []string) []subRequest {
	ave := Ceil(len(paths), m.pgManager.GetPeerCount())
	var start, end = 0, 0
	num := m.pgManager.GetPeerCount()
	if num > len(paths) {
		num = len(paths)
	}
	var requests []subRequest
	for i := 0; i < num; i++ {
		end = start + ave
		if i == num-1 {
			end = len(paths)
		}
		sel, err := util.UnionPathSelector(paths[start:end], true)
		// continue or return
		if err != nil {
			m.returnedErrors <- err
		}
		requests = append(requests, subRequest{
			Root:       m.rootCid,
			Selector:   sel,
			Extensions: m.extensions,
		})
		start = end
	}
	return requests
}

func Ceil(x, y int) int {
	return int(math.Ceil(float64(x) / float64(y)))
}

func (m *ParallelRequestManger) RegisterCollectSpeedInfo(ctx context.Context) {
	//type metrics struct {
	//	lock  sync.Mutex
	//	start int64
	//	cost  int64
	//	size  uint64
	//}
	//ti := make(map[string]*metrics, 1)
	//m.exchange.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
	//	if _, ok := ti[request.ID().String()]; !ok {
	//		ti[request.ID().String()] = &metrics{start: time.Now().UnixNano()}
	//	} else {
	//		fmt.Printf("peer:%s requestID:%s\n", p.String(), request.ID())
	//	}
	//})
	//m.exchange.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
	//	id := p.String() + responseData.RequestID().String()
	//	ti[id].lock.Lock()
	//	defer ti[id].lock.Unlock()
	//	ti[id].cost = time.Now().UnixNano() - ti[id].start
	//	ti[id].size += blockData.BlockSize()
	//	//todo:maybe more efficient
	//	m.pgManager.UpdateSpeed(p.String(), calculateSpeed(ti[id].size, ti[id].cost))
	//})
}

func calculateSpeed(x uint64, y int64) uint64 {
	// byte/(ns/1000000)/1024=kb/ms
	// todo more efficient calculation
	return uint64(math.Ceil(float64(x) / (float64(y) / 1_000_000.0) / 1024))
}
