package pgmanager

import (
	"context"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPeerGroupManager(t *testing.T) {
	peerStrList := []string{
		"12D3KooWHxeZmgsaDquHDCaCRBAs736AMNUqMNrnso5orvjfiPd5",
		"12D3KooWSqu5Kq5L9mSE6jxEgscbhfmR2HxaxnrGSkYqP2NPox4Z",
		"12D3KooWKrz2k92ch1hUryMbt1KBY2v2gnvMLJ9zAGZ53X3hShhN",
		"12D3KooWA3tABN8sGVqEWGxfcRza5bMW7sA3ccNUfZX2zunpGgYP",
	}
	var peers []peer.ID
	for _, pstr := range peerStrList {
		p, _ := peer.Decode(pstr)
		peers = append(peers, p)
	}

	t.Run("Lock peers", func(t *testing.T) {
		pgm := NewPeerGroupManager(peers)
		peerIds := pgm.WaitIdlePeers(context.TODO(), 2)
		for _, id := range peerIds {
			require.Equal(t, false, pgm.GetPeerInfo(id).idle.Load())
		}
	})
	t.Run("Release peers", func(t *testing.T) {
		pgm := NewPeerGroupManager(peers)
		peerIds := pgm.WaitIdlePeers(context.TODO(), 2)
		pgm.ReleasePeers(peerIds)
		for _, id := range peerIds {
			require.Equal(t, true, pgm.GetPeerInfo(id).idle.Load())
		}
	})
	t.Run("Best peers", func(t *testing.T) {
		pgm := NewPeerGroupManager(peers)
		expected := make([]peer.ID, 4)
		peerIds := pgm.WaitIdlePeers(context.TODO(), 3)
		for i, id := range peerIds {
			pgm.UpdateTTFB(id, 100+int64(i))
			if i == 1 {
				pgm.UpdateSpeed(id, 300)
				continue
			}
			pgm.UpdateSpeed(id, 300+int64(i)*100)
		}
		expected[0] = peerIds[2]
		expected[1] = peerIds[0]
		expected[2] = peerIds[1]
		peerIds2 := pgm.WaitIdlePeers(context.TODO(), 1)
		expected[3] = peerIds2[0]
		peerIds = append(peerIds, peerIds2...)
		pgm.ReleasePeers(peerIds)
		peerIds = pgm.WaitIdlePeers(context.TODO(), 4)
		require.EqualValues(t, expected, peerIds)
	})
}
