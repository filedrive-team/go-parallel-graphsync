package pgmanager

import (
	"context"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/atomic"
	"sort"
)

type PeerGroupManager struct {
	peers      map[string]*PeerInfo
	idleNotify chan struct{}
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

func NewPeerGroupManager(peers []peer.ID) *PeerGroupManager {
	mgr := &PeerGroupManager{
		peers:      make(map[string]*PeerInfo),
		idleNotify: make(chan struct{}, len(peers)),
	}
	for _, p := range peers {
		pi := &PeerInfo{
			peer: p,
		}
		pi.idle.Store(true)
		mgr.peers[p.String()] = pi
		mgr.idleNotify <- struct{}{}
	}
	return mgr
}

func (pm *PeerGroupManager) UpdateSpeed(peerId string, transformSpeed uint64) {
	pm.peers[peerId].transformSpeed = transformSpeed
}

func (pm *PeerGroupManager) UpdateDelay(peerId string, timeDelay int64) {
	pm.peers[peerId].timeDelay = timeDelay
}

func (pm *PeerGroupManager) LockPeer(peerId peer.ID) bool {
	if pi, ok := pm.peers[peerId.String()]; ok {
		return pi.idle.CompareAndSwap(true, false)
	}
	return false
}

func (pm *PeerGroupManager) ReleasePeer(peerId peer.ID) {
	if pi, ok := pm.peers[peerId.String()]; ok {
		pi.idle.Store(true)
		pm.idleNotify <- struct{}{}
	}
}

func (pm *PeerGroupManager) ReleasePeers(ids []peer.ID) {
	for _, id := range ids {
		pm.ReleasePeer(id)
	}
}

func (pm *PeerGroupManager) GetPeerInfo(peerId string) *PeerInfo {
	if info, ok := pm.peers[peerId]; ok {
		return info
	}
	return nil
}

func (pm *PeerGroupManager) getIdlePeers(top int) []peer.ID {
	//for _, p := range pm.peers {
	//	fmt.Println("before peerid:", p.peer, " idle:", p.idle.Load())
	//}
	//defer func() {
	//	for _, p := range pm.peers {
	//		fmt.Println("after peerid:", p.peer, " idle:", p.idle.Load())
	//	}
	//}()
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
