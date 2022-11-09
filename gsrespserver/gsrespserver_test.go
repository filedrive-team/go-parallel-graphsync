package gsrespserver

import (
	"context"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"os"
	"path"
	"testing"
	"time"
)

func TestRecordDelay(t *testing.T) {
	keyFile := path.Join(os.TempDir(), "gsrespserver-host-key9620")
	peerkey, err := util.LoadOrInitPeerKey(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		t.Fatal(err)
	}
	opts := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/9620"),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.DefaultTransports,
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	keyFile2 := path.Join(os.TempDir(), "gsrespserver-host-key9621")
	peerkey2, err := util.LoadOrInitPeerKey(keyFile2)
	if err != nil {
		t.Fatal(err)
	}
	cmgr2, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		t.Fatal(err)
	}
	opts2 := []libp2p.Option{
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/9621"),
		libp2p.ConnectionManager(cmgr2),
		libp2p.Identity(peerkey2),
		libp2p.DefaultTransports,
	}

	host2, err := libp2p.New(opts2...)
	if err != nil {
		t.Fatal(err)
	}
	maddr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/9621")
	if err != nil {
		t.Fatal(err)
	}
	addrInfos := []peer.AddrInfo{{host2.ID(), []multiaddr.Multiaddr{maddr}}}
	host.Peerstore().AddAddr(host2.ID(), maddr, peerstore.PermanentAddrTTL)
	err = host.Connect(context.TODO(), peer.AddrInfo{
		ID: host2.ID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	go NewParallelGraphServerManger(addrInfos, host).RecordDelay(ctx, time.Second*5)
	time.Sleep(time.Second * 15)
	cancel()
}
