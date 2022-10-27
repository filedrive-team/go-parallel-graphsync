package example

import (
	"context"
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"testing"
	"time"
)

func TestParGraphSyncRequestManger(t *testing.T) {
	util.StartParGraphSyncRequestManger(context.TODO(), bigCarParExchange, cidlink.Link{Cid: bigCarRootCid}, globalAddrInfos)
}
func TestParGraphSyncRequestMangerSubtree(t *testing.T) {
	sel1, err := textselector.SelectorSpecFromPath("Links/2/Hash/", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	responseProgress, errors := bigCarParExchange.Request(context.TODO(), bigCarAddrInfos[0].ID, cidlink.Link{Cid: bigCarRootCid}, sel1.Node())
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	var ci cidlink.Link
	for blk := range responseProgress {
		if blk.LastBlock.Link != nil {
			ci = blk.LastBlock.Link.(cidlink.Link)
		}
	}
	time.Sleep(time.Second)
	fmt.Println("start")
	util.StartParGraphSyncRequestManger(context.TODO(), bigCarParExchange, ci, globalAddrInfos)
}
