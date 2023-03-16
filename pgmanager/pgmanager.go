package pgmanager

import (
	"context"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/atomic"
	"sort"
)

var log = logging.Logger("pgmanager")

type PeerGroupManager struct {
	peers      map[peer.ID]*PeerInfo
	cache      PeerInfos
	idleNotify chan struct{}
}

type PeerInfo struct {
	peer   peer.ID
	idle   atomic.Bool
	ttfb   int64 // ms
	speed  int64 // B/s
	reqNum int64 // request number
}

func (pi PeerInfo) String() string {
	return fmt.Sprintf("peer: %s idle: %s ttfb: %v ms speed: %v KB/s reqNum: %d", pi.peer, pi.idle.String(), pi.ttfb, pi.speed/1024, pi.reqNum)
}

type PeerInfos []*PeerInfo

func (p PeerInfos) Len() int {
	return len(p)
}

// speed first
func (p PeerInfos) Less(i, j int) bool {
	if p[i].speed == p[j].speed {
		return p[i].ttfb < p[j].ttfb
	}
	return p[i].speed > p[j].speed
}

func (p PeerInfos) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func NewPeerGroupManager(peers []peer.ID) *PeerGroupManager {
	mgr := &PeerGroupManager{
		peers:      make(map[peer.ID]*PeerInfo),
		cache:      make(PeerInfos, 0, len(peers)),
		idleNotify: make(chan struct{}, len(peers)),
	}
	for _, p := range peers {
		pi := &PeerInfo{
			peer: p,
		}
		pi.idle.Store(true)
		mgr.peers[p] = pi
		mgr.cache = append(mgr.cache, pi)
		mgr.idleNotify <- struct{}{}
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
		pm.idleNotify <- struct{}{}
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
		case <-pm.idleNotify:
			ids := pm.getIdlePeers(top)
			// removes a message that has been used up
			remove := len(ids) - 1
			for i := 0; i < remove; i++ {
				<-pm.idleNotify
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

func (pm *PeerGroupManager) Close() {
	close(pm.idleNotify)
}
