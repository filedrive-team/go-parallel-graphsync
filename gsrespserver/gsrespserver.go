package gsrespserver

import (
	"context"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/libp2p/go-libp2p-core/peer"
	"sync"
)

type ParallelGraphServerManger struct {
	lock             sync.RWMutex
	ParaGraphServers map[string]*ParallelGraphServer
}
type ParallelGraphServer struct {
	lock           sync.Mutex
	dealCount      int64
	timeDelay      int64
	transformSpeed int64
	addrInfo       peer.AddrInfo
}

func NewParallelGraphServerManger(infos []peer.AddrInfo) *ParallelGraphServerManger {
	var parallelGraphServerManger ParallelGraphServerManger
	parallelGraphServers := make(map[string]*ParallelGraphServer)
	for _, info := range infos {
		parallelGraphServers[info.ID.String()] = &ParallelGraphServer{
			dealCount: 0,
			addrInfo:  info,
		}
	}
	parallelGraphServerManger.ParaGraphServers = parallelGraphServers
	return &parallelGraphServerManger
}
func (pm *ParallelGraphServerManger) UpdateInfo(ctx context.Context, peerId string, timeDelay, transformSpeed int64) {
	pm.ParaGraphServers[peerId].lock.Lock()
	pm.ParaGraphServers[peerId].timeDelay = timeDelay
	pm.ParaGraphServers[peerId].transformSpeed = transformSpeed
	pm.ParaGraphServers[peerId].lock.Unlock()
}
func (pm *ParallelGraphServerManger) RemovePeer(ctx context.Context, peerId string) {
	delete(pm.ParaGraphServers, peerId)
}
func (pm *ParallelGraphServerManger) UpdateDealCount(ctx context.Context, peerId string, count int64) {
	pm.ParaGraphServers[peerId].lock.Lock()
	pm.ParaGraphServers[peerId].dealCount = count
	pm.ParaGraphServers[peerId].lock.Unlock()
}
func (pm *ParallelGraphServerManger) FreeDealCount(ctx context.Context, params []pargraphsync.RequestParam) {
	for _, param := range params {
		pm.ParaGraphServers[param.PeerId.String()].lock.Lock()
		pm.ParaGraphServers[param.PeerId.String()].dealCount--
		pm.ParaGraphServers[param.PeerId.String()].lock.Unlock()
	}
}
func (pm *ParallelGraphServerManger) GetIdlePeer(ctx context.Context) peer.ID {
	pm.lock.RLock()
	defer pm.lock.RUnlock()
	dealCount := int64(1_000_000)
	var peerId peer.ID
	for _, pgDServer := range pm.ParaGraphServers {
		if pgDServer.dealCount < dealCount {
			dealCount = pgDServer.dealCount
			peerId = pgDServer.addrInfo.ID
		}
	}
	//fmt.Println("IdlePeer:", peerId.String())
	pm.ParaGraphServers[peerId.String()].dealCount++
	return peerId
}
func (pm *ParallelGraphServerManger) GetPeerCount(ctx context.Context) int {
	return len(pm.ParaGraphServers)
}
