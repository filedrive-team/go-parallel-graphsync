package example

import (
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"os"
	"path"
	"testing"
)

func TestDivideSelector(t *testing.T) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	bs, err := loadCarV2Blockstore("big-v2.car")
	if err != nil {
		t.Fatal(err)
	}
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, 3, 9820, bs, false)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-key9720")
	rootCid, _ := cid.Parse("QmSvtt6abwrp3MybYqHHA4BdFjjuLBABXjLEVQKpMUfUU8")
	host, pgs, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9720", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}

	pgs.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})
	for _, addr := range addrInfos {
		host.Peerstore().AddAddr(addr.ID, addr.Addrs[0], peerstore.PermanentAddrTTL)
	}

	var s = util.ParGSTask{
		Gs:           pgs,
		AddedTasks:   make(map[string]struct{}),
		StartedTasks: make(map[string]struct{}),
		RunningTasks: make(chan util.Tasks, 1),
		DoneTasks:    make(map[string]struct{}),
		Root:         cidlink.Link{Cid: rootCid},
		PeerIds:      addrInfos,
	}

	testCases := []struct {
		name      string
		links     []string
		num       []int64
		paths     []string
		expectRes bool
	}{
		{
			name:  "normal-true",
			links: []string{"Links", "Links/0/Hash/Links"},
			num:   []int64{3, 2},
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
			},
			expectRes: true,
		},
		{
			name:  "normal-false",
			links: []string{"Links", "Links/0/Hash/Links"},
			num:   []int64{3, 2},
			paths: []string{
				"Links/0/Hash/Links/0/Hash",
				"Links/0/Hash/Links/1/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/3/Hash",
			},
			expectRes: false,
		},
		{
			name:  "more-true",
			links: []string{"Links", "Links/1/Hash/Links"},
			num:   []int64{5, 4},
			paths: []string{
				"Links/1/Hash/Links/0/Hash",
				"Links/1/Hash/Links/1/Hash",
				"Links/1/Hash/Links/2/Hash",
				"Links/1/Hash/Links/3/Hash",
				"Links/0/Hash",
				"Links/1/Hash",
				"Links/2/Hash",
				"Links/3/Hash",
				"Links/4/Hash",
			},
			expectRes: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var pathMap = make(map[string]int64)
			for i, link := range testCase.links {
				pathMap[link] = testCase.num[i]
			}
			s.CollectTasks(pathMap)
			s.StartRun(context.TODO())
			if compare(s.DoneTasks, testCase.paths) != testCase.expectRes {
				t.Fatal("not equal")
			}
		})
	}

}
func compare(doneTasks map[string]struct{}, paths2 []string) bool {
	for _, pa2 := range paths2 {
		have := false
		for pa1 := range doneTasks {
			if pa1 == pa2 {
				have = true
				break
			}
		}
		if have {
			continue
		} else {
			return false
		}
	}
	return true
}
