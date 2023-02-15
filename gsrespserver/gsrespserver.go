package gsrespserver

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/atomic"
	"sort"
	"time"
)

type PeersGroupManager struct {
	peers map[string]*PeerInfo
}

type PeerInfo struct {
	peer           peer.ID
	idle           atomic.Bool
	timeDelay      int64
	transformSpeed uint64
}

type PeerInfos []*PeerInfo

func (p PeerInfos) Len() int {
	return len(p)
}

func (p PeerInfos) Less(i, j int) bool {
	return p[i].transformSpeed < p[j].transformSpeed
}

func (p PeerInfos) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func NewPeersGroupManager(peers []peer.ID) *PeersGroupManager {
	mgr := &PeersGroupManager{
		peers: make(map[string]*PeerInfo),
	}
	for _, p := range peers {
		pi := &PeerInfo{
			peer: p,
		}
		pi.idle.Store(true)
		mgr.peers[p.String()] = pi
	}
	return mgr
}

func (pm *PeersGroupManager) UpdateSpeed(peerId string, transformSpeed uint64) {
	pm.peers[peerId].transformSpeed = transformSpeed
}

func (pm *PeersGroupManager) UpdateDelay(peerId string, timeDelay int64) {
	pm.peers[peerId].timeDelay = timeDelay
}

func (pm *PeersGroupManager) LockPeer(peerId peer.ID) bool {
	if pi, ok := pm.peers[peerId.String()]; ok {
		return pi.idle.CompareAndSwap(true, false)
	}
	return false
}

func (pm *PeersGroupManager) ReleasePeer(peerId peer.ID) {
	if pi, ok := pm.peers[peerId.String()]; ok {
		pi.idle.Store(true)
	}
}

func (pm *PeersGroupManager) ReleasePeers(ids []peer.ID) {
	for _, id := range ids {
		pm.ReleasePeer(id)
	}
}

func (pm *PeersGroupManager) GetPeerInfo(peerId string) *PeerInfo {
	if info, ok := pm.peers[peerId]; ok {
		return info
	}
	return nil
}

func (pm *PeersGroupManager) GetIdlePeers(top int) []peer.ID {
	for _, p := range pm.peers {
		fmt.Println("before peerid:", p.peer, " idle:", p.idle.Load())
	}
	defer func() {
		for _, p := range pm.peers {
			fmt.Println("after peerid:", p.peer, " idle:", p.idle.Load())
		}
	}()
	resList := make([]peer.ID, 0, top)
	list := make(PeerInfos, 0, len(pm.peers))
	for _, p := range pm.peers {
		list = append(list, p)
	}
	sort.Sort(list)
	for _, p := range list {
		if pm.LockPeer(p.peer) {
			resList = append(resList, p.peer)
			if len(resList) == top {
				return resList
			}
		}
	}
	return resList
}

func (pm *PeersGroupManager) WaitIdlePeers(ctx context.Context, top int) []peer.ID {
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ids := pm.GetIdlePeers(top)
			if len(ids) > 0 {
				return ids
			}
		}
	}
}

func (pm *PeersGroupManager) GetPeerCount() int {
	return len(pm.peers)
}

func (pm *PeersGroupManager) IsAllIdle() bool {
	for _, p := range pm.peers {
		if !p.idle.Load() {
			return false
		}
	}
	return true
}
