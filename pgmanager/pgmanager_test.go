package pgmanager

import (
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"os"
	"path"
	"testing"
)

var pgsm *PeerGroupManager

func TestMain(m *testing.M) {
	keyFile := path.Join(os.TempDir(), "gsrespserver-host-key9620")
	peerkey, err := util.LoadOrInitPeerKey(keyFile)
	if err != nil {
		panic(err)
	}
	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		panic(err)
	}
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/9620"),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.DefaultTransports,
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		panic(err)
	}

	var peerIds []peer.ID
	for i := 0; i < 3; i++ {
		keyFile2 := path.Join(os.TempDir(), fmt.Sprintf("gsrespserver-host-key%v", 9630+i))
		peerkey2, err1 := util.LoadOrInitPeerKey(keyFile2)
		if err1 != nil {
			panic(err1)
		}
		cmgr2, err1 := connmgr.NewConnManager(2000, 3000)
		if err1 != nil {
			panic(err1)
		}
		opts2 := []libp2p.Option{
			libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%v", 9630+i)),
			libp2p.ConnectionManager(cmgr2),
			libp2p.Identity(peerkey2),
			libp2p.DefaultTransports,
		}

		host2, err1 := libp2p.New(opts2...)
		if err1 != nil {
			panic(err1)
		}
		maddr, err1 := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%v", 9630+i))
		if err1 != nil {
			panic(err1)
		}
		peerIds = append(peerIds, host2.ID())
		host.Peerstore().AddAddr(host2.ID(), maddr, peerstore.PermanentAddrTTL)
		err = host.Connect(context.TODO(), peer.AddrInfo{
			ID: host2.ID(),
		})
		if err != nil {
			panic(err1)
		}
	}
	pgsm = NewPeerGroupManager(peerIds)
	os.Exit(m.Run())
}

// TODO: fix me
//func TestRecordDelay(t *testing.T) {
//	ctx := context.Background()
//	ctx, cancel := context.WithCancel(ctx)
//	go pgsm.RecordDelay(ctx, time.Second*5)
//	time.Sleep(time.Second * 15)
//	cancel()
//	for _, pgs := range pgsm.ParaGraphServers {
//		require.NotEqual(t, 0, pgs.peerInfo.transformSpeed)
//	}
//
//}
//func TestUpdate(t *testing.T) {
//	var speed uint64 = 100
//	var delay, dealCount int64 = 10, 20
//	var ctx = context.Background()
//	pgsm.UpdateSpeed(ctx, addrInfos[0].ID.String(), speed)
//	pgsm.UpdateDelay(ctx, addrInfos[0].ID.String(), delay)
//	pgsm.UpdateDealCount(ctx, addrInfos[0].ID.String(), dealCount)
//	pgsm.UpdateDealCount(ctx, addrInfos[1].ID.String(), dealCount)
//	peerId := pgsm.GetIdlePeer(ctx)
//	require.Equal(t, addrInfos[2].ID, peerId)
//	info := pgsm.GetPeerInfo(context.TODO(), addrInfos[0].ID.String())
//	require.Equal(t, PeerInfo{
//		dealCount:      dealCount,
//		timeDelay:      delay,
//		transformSpeed: speed,
//		addrInfo:       info.addrInfo,
//	}, info)
//	require.Equal(t, 3, pgsm.GetPeerCount(ctx))
//	pgsm.FreeDealCount(ctx, []pargraphsync.RequestParam{{PeerId: addrInfos[0].ID}})
//	require.Equal(t, int64(19), pgsm.GetPeerInfo(context.TODO(), addrInfos[0].ID.String()).dealCount)
//	pgsm.RemovePeer(ctx, addrInfos[0].ID.String())
//	info = pgsm.GetPeerInfo(context.TODO(), addrInfos[0].ID.String())
//	require.Equal(t, PeerInfo{}, info)
//}
