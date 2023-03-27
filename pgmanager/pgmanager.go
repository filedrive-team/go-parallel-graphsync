package pgmanager

import (
	"context"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/atomic"
	"sort"
	"sync"
)

var log = logging.Logger("pgmanager")

type PeerGroupManager struct {
	peers        map[peer.ID]*PeerInfo
	cache        PeerInfos
	idleNotify   chan *PeerInfo
	idleCallback func()
}

type PeerInfo struct {
	peer              peer.ID
	idle              atomic.Bool
	wait              atomic.Bool // in wait period
	waitCounter       int32
	waitCounterLocker sync.Mutex
	ttfb              int64 // ms
	speed             int64 // B/s
	reqNum            int64 // request number
}

func (pi PeerInfo) String() string {
	return fmt.Sprintf("peer: %s idle: %s ttfb: %v ms speed: %v KB/s reqNum: %d", pi.peer, pi.idle.String(), pi.ttfb, pi.speed/1024, pi.reqNum)
}

type PeerInfos []*PeerInfo

func (p PeerInfos) Len() int {
	return len(p)
}

// ttfb first
func (p PeerInfos) Less(i, j int) bool {
	if p[i].ttfb == p[j].ttfb {
		return p[i].speed > p[j].speed
	}
	return p[i].ttfb < p[j].ttfb
}

func (p PeerInfos) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func NewPeerGroupManager(peers []peer.ID) *PeerGroupManager {
	mgr := &PeerGroupManager{
		peers:      make(map[peer.ID]*PeerInfo),
		cache:      make(PeerInfos, 0, len(peers)),
		idleNotify: make(chan *PeerInfo, len(peers)),
	}
	for _, p := range peers {
		pi := &PeerInfo{
			peer: p,
		}
		pi.idle.Store(true)
		mgr.peers[p] = pi
		mgr.cache = append(mgr.cache, pi)
		mgr.idleNotify <- pi
	}
	return mgr
}

func (pm *PeerGroupManager) UpdateSpeed(peerId peer.ID, transformSpeed int64) {
	log.Debugf("metrics peerid: %v speed: %v KB/s", peerId, transformSpeed/1024)
	if info, ok := pm.peers[peerId]; ok {
		info.speed = transformSpeed
	} else {
		log.Errorf("metrics UpdateSpeed peerid: %v not exist", peerId)
	}
}

func (pm *PeerGroupManager) UpdateTTFB(peerId peer.ID, ttfb int64) {
	log.Debugf("metrics peerid: %v ttfb: %v ms", peerId, ttfb)
	if info, ok := pm.peers[peerId]; ok {
		info.ttfb = ttfb
	} else {
		log.Errorf("metrics UpdateTTFB peerid: %v not exist", peerId)
	}
}

func (pm *PeerGroupManager) LockPeer(peerId peer.ID) bool {
	if pi, ok := pm.peers[peerId]; ok {
		return pi.idle.CompareAndSwap(true, false)
	}
	return false
}

func (pm *PeerGroupManager) ReleasePeer(peerId peer.ID) {
	if pi, ok := pm.peers[peerId]; ok {
		pi.idle.Store(true)
		pm.idleNotify <- pi
		if pm.idleCallback != nil {
			pm.idleCallback()
		}
	}
}

func (pm *PeerGroupManager) ReleasePeers(ids []peer.ID) {
	for _, id := range ids {
		pm.ReleasePeer(id)
	}
}

func (pm *PeerGroupManager) GetPeerInfo(peerId peer.ID) *PeerInfo {
	if info, ok := pm.peers[peerId]; ok {
		return info
	}
	return nil
}

func (pm *PeerGroupManager) GetPeerInfoList() PeerInfos {
	return pm.cache
}

func (pm *PeerGroupManager) getIdlePeers(top int) []peer.ID {
	resList := make([]peer.ID, 0, top)
	sort.Sort(pm.cache)
	for _, p := range pm.cache {
		// check whether the peers have passed the waiting period
		if p.wait.Load() {
			p.waitCounterLocker.Lock()
			p.waitCounter -= 1
			if p.waitCounter == 0 {
				p.wait.Store(true)
			}
			p.waitCounterLocker.Unlock()
			continue
		}
		if pm.LockPeer(p.peer) {
			p.reqNum += 1
			resList = append(resList, p.peer)
			if len(resList) == top {
				return resList
			}
		}
	}
	return resList
}

func (pm *PeerGroupManager) WaitIdlePeers(ctx context.Context, top int) []peer.ID {
	if top <= 0 {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case pi := <-pm.idleNotify:
			if pi.wait.Load() {
				continue
			}
			ids := pm.getIdlePeers(top)
			// removes a message that has been used up
			remove := len(ids) - 1
			count := 0
			for count < remove {
				pi = <-pm.idleNotify
				if pi.wait.Load() {
					continue
				}
				count += 1
			}
			return ids
		}
	}
}

func (pm *PeerGroupManager) GetPeerCount() int {
	return len(pm.peers)
}

func (pm *PeerGroupManager) IsAllIdle() bool {
	for _, p := range pm.peers {
		if !p.idle.Load() {
			return false
		}
	}
	return true
}

func (pm *PeerGroupManager) HasIdlePeer() bool {
	for _, p := range pm.peers {
		if p.idle.Load() {
			return true
		}
	}
	return false
}

func (pm *PeerGroupManager) Close() {
	close(pm.idleNotify)
}

func (pm *PeerGroupManager) IsBestPeer(p peer.ID) bool {
	sort.Sort(pm.cache)
	pi := pm.GetPeerInfo(p)
	if pi == nil {
		return false
	}
	for _, item := range pm.cache {
		if item.idle.Load() && !item.wait.Load() {
			if pi.speed == 0 && float64(item.ttfb) < float64(pi.ttfb) {
				return false
			}
			if float64(item.speed) > 1.5*float64(pi.speed) {
				return false
			}
		}
	}
	return true
}

func (pm *PeerGroupManager) SleepPeer(peerId peer.ID) {
	if pi, ok := pm.peers[peerId]; ok {
		pi.waitCounterLocker.Lock()
		pi.waitCounter = 1
		pi.waitCounterLocker.Unlock()
		pi.wait.Store(true)
	}
}

func (pm *PeerGroupManager) RegisterIdleCallback(callback func()) {
	pm.idleCallback = callback
}

func (pm *PeerGroupManager) GetPeerTimeout() int64 {
	ttfbList := make([]int, 0, len(pm.cache))
	for _, item := range pm.cache {
		if item.ttfb == 0 {
			continue
		}
		ttfbList = append(ttfbList, int(item.ttfb))
	}
	if len(ttfbList) == 0 {
		return 1000
	}
	sort.Ints(ttfbList)
	return int64(float64(ttfbList[0]) * 1.5)
}
