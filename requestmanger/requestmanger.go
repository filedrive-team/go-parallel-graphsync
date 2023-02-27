package requestmanger

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/filedrive-team/go-parallel-graphsync/pgmanager"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/filedrive-team/go-parallel-graphsync/util/parseselector"
	"github.com/ipfs/go-graphsync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/peer"
	"math"
	"strings"
	"sync"
	"time"
)

var log = logging.Logger("parrequestmanger")

type subRequest struct {
	Root       ipld.Link
	Selector   ipld.Node
	Path       string
	Extensions []graphsync.ExtensionData

	ResolveSubtreeRoot bool
	Recursive          bool
}

type ParallelRequestManger struct {
	requestChan chan []subRequest
	// save request, contains status: requesting, completed
	inProgressReq sync.Map // key: sha256(root+selector)

	exchange   pargraphsync.ParallelGraphExchange
	parentCtx  context.Context
	pgManager  *pgmanager.PeerGroupManager
	rootCid    ipld.Link
	selector   ipld.Node
	extensions []graphsync.ExtensionData

	returnedResponses chan graphsync.ResponseProgress
	returnedErrors    chan error
}

func NewParGraphSyncRequestManger(exchange pargraphsync.ParallelGraphExchange, parent context.Context, peers []peer.ID,
	root ipld.Link, selector ipld.Node, extensions ...graphsync.ExtensionData) *ParallelRequestManger {
	if len(peers) == 0 {
		panic("have no peer")
	}
	return &ParallelRequestManger{
		requestChan:       make(chan []subRequest, 1024),
		exchange:          exchange,
		parentCtx:         parent,
		rootCid:           root,
		selector:          selector,
		extensions:        extensions,
		pgManager:         pgmanager.NewPeerGroupManager(peers),
		returnedResponses: make(chan graphsync.ResponseProgress),
		returnedErrors:    make(chan error),
	}
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
			peerId := m.pgManager.WaitIdlePeers(ctx, 1)
			if len(peerId) == 0 {
				return m.singleErrorResponse(errors.New("no idle peer"))
			}
			return m.exchange.Request(ctx, peerId[0], m.rootCid, m.selector, m.extensions...)
		}
	}

	m.exchange.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		log.Infof("NetworkErrorListener peer=%s request requestId=%s error=%v", p.String(), request.ID().String(), err)
		m.exchange.CancelSubRequest(ctx, request.ID())
	})
	m.RegisterCollectSpeedInfo(ctx)

	go func() {
		defer func() {
			m.close()
		}()

		// collect subtree root cid
		if isAllSel {
			if sel, err := util.RootLeftSelector(""); err != nil {
				m.returnedErrors <- err
				return
			} else {
				m.pushSubRequest(ctx, []subRequest{{
					Root:       m.rootCid,
					Selector:   sel,
					Extensions: m.extensions,
				}})
			}
		} else {
			subtreeRootReqs := make([]subRequest, 0, len(selectors))
			for _, sel := range selectors {
				subtreeRootReqs = append(subtreeRootReqs, subRequest{
					Root:               m.rootCid,
					Selector:           sel.Sel,
					Path:               sel.Path,
					Extensions:         m.extensions,
					Recursive:          sel.Recursive,
					ResolveSubtreeRoot: true,
				})
			}
			m.pushSubRequest(ctx, subtreeRootReqs)
		}

		m.handleRequest(ctx)
	}()
	return m.collectResponses(ctx, m.returnedResponses, m.returnedErrors, func() {
		// cancel request
	})
}

func (m *ParallelRequestManger) collectResponses(
	requestCtx context.Context,
	incomingResponses <-chan graphsync.ResponseProgress,
	incomingErrors <-chan error,
	cancelRequest func(),
) (<-chan graphsync.ResponseProgress, <-chan error) {

	returnedResponses := make(chan graphsync.ResponseProgress)
	returnedErrors := make(chan error)

	go func() {
		var receivedResponses []graphsync.ResponseProgress
		defer close(returnedResponses)
		outgoingResponses := func() chan<- graphsync.ResponseProgress {
			if len(receivedResponses) == 0 {
				return nil
			}
			return returnedResponses
		}
		nextResponse := func() graphsync.ResponseProgress {
			if len(receivedResponses) == 0 {
				return graphsync.ResponseProgress{}
			}
			return receivedResponses[0]
		}
		for len(receivedResponses) > 0 || incomingResponses != nil {
			select {
			case <-m.parentCtx.Done():
				return
			case <-requestCtx.Done():
				if incomingResponses != nil {
					cancelRequest()
				}
				return
			case response, ok := <-incomingResponses:
				if !ok {
					incomingResponses = nil
				} else {
					receivedResponses = append(receivedResponses, response)
				}
			case outgoingResponses() <- nextResponse():
				receivedResponses = receivedResponses[1:]
			}
		}
	}()
	go func() {
		var receivedErrors []error
		defer close(returnedErrors)

		outgoingErrors := func() chan<- error {
			if len(receivedErrors) == 0 {
				return nil
			}
			return returnedErrors
		}
		nextError := func() error {
			if len(receivedErrors) == 0 {
				return nil
			}
			return receivedErrors[0]
		}

		for len(receivedErrors) > 0 || incomingErrors != nil {
			select {
			case <-m.parentCtx.Done():
				return
			case <-requestCtx.Done():
				select {
				case <-m.parentCtx.Done():
				case returnedErrors <- graphsync.RequestClientCancelledErr{}:
				}
				return
			case err, ok := <-incomingErrors:
				if !ok {
					incomingErrors = nil
					// even if the `incomingErrors` channel is closed without any error,
					// the context could still have timed out in which case we need to inform the caller of the same.
					select {
					case <-requestCtx.Done():
						select {
						case <-m.parentCtx.Done():
						case returnedErrors <- graphsync.RequestClientCancelledErr{}:
						}
					default:
					}
				} else {
					receivedErrors = append(receivedErrors, err)
				}
			case outgoingErrors() <- nextError():
				receivedErrors = receivedErrors[1:]
			}
		}
	}()
	return returnedResponses, returnedErrors
}

func (m *ParallelRequestManger) syncSubtreeRoot(ctx context.Context, p peer.ID, request subRequest, exitCh chan<- struct{}) {
	defer func() {
		log.Debugf("finish subrequest, selector: %s", util.SelectorToJson(request.Selector))
	}()
	responseProgress, errorChan := m.exchange.Request(ctx, p, request.Root, request.Selector, request.Extensions...)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		for e := range errorChan {
			if _, ok := e.(graphsync.RequestClientCancelledErr); ok {
				// retry request
				m.pushSubRequest(ctx, []subRequest{request})
				return
			}
			m.returnedErrors <- e
			// cancel this group request
			exitCh <- struct{}{}
			return
		}
	}()
	for blk := range responseProgress {
		m.returnedResponses <- blk

		if request.Path == blk.Path.String() {
			log.Debugf("recursive:%v path=%s", request.Recursive, blk.Path.String())
			if blk.LastBlock.Link != nil {
				// TODO: check if blk have sub node,Need to check? Or is this correct to check?
				recursive := request.Recursive
				if nd, err := blk.Node.LookupByString("Links"); err != nil || nd.Length() == 0 {
					recursive = false
				}
				if recursive {
					if sel, err := util.RootLeftSelector(""); err != nil {
						m.returnedErrors <- err
						// cancel this group request
						exitCh <- struct{}{}
						return
					} else {
						m.pushSubRequest(ctx, []subRequest{{
							Root:       blk.LastBlock.Link,
							Selector:   sel,
							Extensions: m.extensions,
						}})
					}
				}
			}
		}
	}
}

func (m *ParallelRequestManger) handleRequest(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 25)
	defer ticker.Stop()
	var wg sync.WaitGroup
	exitCh := make(chan struct{})
	defer func() {
		go func() {
			// remove unnecessary exit signals
			for _ = range exitCh {
			}
		}()
		wg.Wait()
		close(exitCh)
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case request := <-m.requestChan:
			// get the idle peers
			peers := m.pgManager.WaitIdlePeers(ctx, len(request))
			if len(peers) == 0 {
				// exit
				return
			}
			// TODO: merge multiple existing requests based on the number of idle peers
			// len(m.requestChan)
			for i, p := range peers {
				wg.Add(1)
				go func(index int, id peer.ID) {
					defer wg.Done()
					defer m.pgManager.ReleasePeer(id)
					if request[index].ResolveSubtreeRoot {
						m.syncSubtreeRoot(ctx, id, request[index], exitCh)
					} else {
						m.syncData(ctx, id, request[index], exitCh)
					}
				}(i, p)
			}
			remain := request[len(peers):]
			if len(remain) > 0 {
				m.pushSubRequest(ctx, remain)
			}
		case <-m.parentCtx.Done():
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// check twice
			if len(m.requestChan) == 0 && m.pgManager.IsAllIdle() && len(m.requestChan) == 0 {
				// sync finished
				return
			}
		case <-exitCh:
			// handle exit signals
			return
		}
	}
}

func (m *ParallelRequestManger) pushSubRequest(ctx context.Context, reqs []subRequest) {
	for _, req := range reqs {
		log.Debugf("push request, root:%v selector: %v", req.Root.String(), util.SelectorToJson(req.Selector))
	}
	select {
	case m.requestChan <- reqs:
	default:
		go func() {
			select {
			case m.requestChan <- reqs:
			case <-ctx.Done():
				return
			}
		}()
	}
}

func generateKey(root ipld.Link, sel ipld.Node) string {
	var s strings.Builder
	s.WriteString(root.String())
	s.WriteString(util.SelectorToJson(sel))
	h := sha256.New()
	h.Write([]byte(s.String()))
	return string(h.Sum(nil))
}

func (m *ParallelRequestManger) syncData(ctx context.Context, p peer.ID, request subRequest, exitCh chan<- struct{}) {
	log.Debugf("subrequest, selector: %s", util.SelectorToJson(request.Selector))
	defer func() {
		log.Debugf("finish subrequest, selector: %s", util.SelectorToJson(request.Selector))
	}()
	responseProgress, errorsChan := m.exchange.Request(ctx, p, request.Root, request.Selector, request.Extensions...)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		for e := range errorsChan {
			if _, ok := e.(graphsync.RequestClientCancelledErr); ok {
				// retry request
				m.pushSubRequest(ctx, []subRequest{request})
				return
			}
			m.returnedErrors <- e
			// cancel this group request
			exitCh <- struct{}{}
			return
		}
	}()

	for blk := range responseProgress {
		m.returnedResponses <- blk

		if nd, err := blk.Node.LookupByString("Links"); err == nil && nd.Length() > 1 {
			path := blk.Path.String()
			links := nd.Length()
			peers := int64(m.pgManager.GetPeerCount())
			requests := make([]subRequest, 0, peers)
			usedLinks := int64(0)
			for index := int64(0); index < peers; index++ {
				remainLinks := links - usedLinks
				if remainLinks <= 0 {
					break
				}
				remainPeers := peers - index
				avg := remainLinks / remainPeers
				if remainLinks%remainPeers != 0 {
					avg += 1
				}

				start := usedLinks
				end := start + avg
				usedLinks += avg

				if sel, err := util.GenerateLeftSubRangeSelector(path, start, end); err != nil {
					m.returnedErrors <- err
					// cancel this group request
					exitCh <- struct{}{}
					return
				} else {
					if _, loaded := m.inProgressReq.LoadOrStore(generateKey(request.Root, sel), struct{}{}); !loaded {
						subPath := ""
						// fill in the path field when there is only one ipld node
						if end-start == 1 {
							if path == "" {
								subPath = "Links"
							} else {
								subPath = path + "/Links"
							}
							subPath = fmt.Sprintf("%s/%d/Hash/Links", subPath, start)
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

// close the Manger
func (m *ParallelRequestManger) close() {
	close(m.returnedErrors)
	close(m.returnedResponses)
	close(m.requestChan)
	m.pgManager.Close()
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
