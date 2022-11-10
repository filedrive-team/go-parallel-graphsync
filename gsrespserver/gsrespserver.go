package gsrespserver

import (
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"sync"
	"time"
)

type ParallelGraphServerManger struct {
	lock             sync.RWMutex
	host             host.Host
	ParaGraphServers map[string]*ParallelGraphServer
}
type ParallelGraphServer struct {
	lock     sync.RWMutex
	peerInfo PeerInfo
}
type PeerInfo struct {
	dealCount      int64
	timeDelay      int64
	transformSpeed uint64
	addrInfo       peer.AddrInfo
}

func NewParallelGraphServerManger(infos []peer.AddrInfo, host host.Host) *ParallelGraphServerManger {
	var parallelGraphServerManger ParallelGraphServerManger
	parallelGraphServers := make(map[string]*ParallelGraphServer)
	for _, info := range infos {
		parallelGraphServers[info.ID.String()] = &ParallelGraphServer{peerInfo: PeerInfo{
			dealCount: 0,
			addrInfo:  info,
		},
		}
	}
	parallelGraphServerManger.ParaGraphServers = parallelGraphServers
	parallelGraphServerManger.host = host
	return &parallelGraphServerManger
}
func (pm *ParallelGraphServerManger) UpdateSpeed(ctx context.Context, peerId string, transformSpeed uint64) {
	pm.ParaGraphServers[peerId].lock.Lock()
	pm.ParaGraphServers[peerId].peerInfo.transformSpeed = transformSpeed
	pm.ParaGraphServers[peerId].lock.Unlock()
	//fmt.Printf("peerId %v,speed: %d\n", peerId, transformSpeed)
}
func (pm *ParallelGraphServerManger) UpdateDelay(ctx context.Context, peerId string, timeDelay int64) {
	pm.ParaGraphServers[peerId].lock.Lock()
	pm.ParaGraphServers[peerId].peerInfo.timeDelay = timeDelay
	pm.ParaGraphServers[peerId].lock.Unlock()
}
func (pm *ParallelGraphServerManger) RemovePeer(ctx context.Context, peerId string) {
	// need check if dealCount==0
	pm.lock.Lock()
	delete(pm.ParaGraphServers, peerId)
	pm.lock.Unlock()
}
func (pm *ParallelGraphServerManger) UpdateDealCount(ctx context.Context, peerId string, count int64) {
	pm.ParaGraphServers[peerId].lock.Lock()
	pm.ParaGraphServers[peerId].peerInfo.dealCount = count
	pm.ParaGraphServers[peerId].lock.Unlock()
}
func (pm *ParallelGraphServerManger) GetPeerInfo(ctx context.Context, peerId string) PeerInfo {
	info, ok := pm.ParaGraphServers[peerId]
	if ok {
		return info.peerInfo
	}
	return PeerInfo{}
}
func (pm *ParallelGraphServerManger) FreeDealCount(ctx context.Context, params []pargraphsync.RequestParam) {
	for _, param := range params {
		pm.ParaGraphServers[param.PeerId.String()].lock.Lock()
		pm.ParaGraphServers[param.PeerId.String()].peerInfo.dealCount--
		pm.ParaGraphServers[param.PeerId.String()].lock.Unlock()
	}
}
func (pm *ParallelGraphServerManger) GetIdlePeer(ctx context.Context) peer.ID {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	dealCount := int64(1_000_000)
	var peerId peer.ID
	for _, pgDServer := range pm.ParaGraphServers {
		if pgDServer.peerInfo.dealCount < dealCount {
			dealCount = pgDServer.peerInfo.dealCount
			peerId = pgDServer.peerInfo.addrInfo.ID
		}
	}
	//fmt.Println("IdlePeer:", peerId.String())
	pm.ParaGraphServers[peerId.String()].peerInfo.dealCount++
	return peerId
}
func (pm *ParallelGraphServerManger) GetPeerCount(ctx context.Context) int {
	return len(pm.ParaGraphServers)
}
func (pm *ParallelGraphServerManger) RecordDelay(ctx context.Context, pingInterval time.Duration) {
	timer := time.NewTimer(pingInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("close")
			return
		case <-timer.C:
			for _, pgs := range pm.ParaGraphServers {
				ps := ping.NewPingService(pm.host)
				ts := ps.Ping(ctx, pgs.peerInfo.addrInfo.ID)
				select {
				case res := <-ts:
					if res.Error != nil {
						fmt.Println("err:", res.Error)
						continue
					}
					fmt.Printf("ID:%v,ping took:%v\n", pgs.peerInfo.addrInfo.ID, res.RTT.Nanoseconds())
					pm.UpdateDelay(ctx, pgs.peerInfo.addrInfo.ID.String(), res.RTT.Nanoseconds())

				case <-time.After(time.Second * 2):
					fmt.Println("failed to receive ping")
				}
			}
			timer.Reset(pingInterval)
		}
	}
}
