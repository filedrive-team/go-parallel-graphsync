package example

import (
	"context"
	crand "crypto/rand"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync"
	graphsyncImpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	carv2bs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

var globalHost host.Host
var globalParExchange pargraphsync.ParallelGraphExchange
var globalAddrInfos []peer.AddrInfo
var globalRoot cid.Cid
var globalBs blockstore.Blockstore

var bigCarRootCid cid.Cid
var bigCarParExchange pargraphsync.ParallelGraphExchange
var bigCarAddrInfos []peer.AddrInfo

const ServicesNum = 3

func TestWrapV1File(t *testing.T) {
	err := car.WrapV1File("./bafkreifehqnkqvw5nwrcj4rg5d3z6q6psspfgu6t52uztwehop63tfybzm.car", "./xx-car-v2.car")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	mainCtx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	addrInfos, err := startSomeGraphSyncServices(mainCtx, ServicesNum, 9010, false, "car-v2.car")
	if err != nil {
		panic(err)
	}
	globalAddrInfos = addrInfos

	keyFile := path.Join(os.TempDir(), "gs-key")
	ds := datastore.NewMapDatastore()
	globalBs = blockstore.NewBlockstore(dssync.MutexWrap(ds))

	globalHost, globalParExchange, err = startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9020", keyFile, globalBs)
	if err != nil {
		panic(err)
	}
	fmt.Printf("requester peerId=%s\n", globalHost.ID())

	globalParExchange.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
	})
	globalParExchange.RegisterIncomingResponseHook(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
		reqId := responseData.RequestID().String()
		status := responseData.Status().String()
		fmt.Printf("RegisterIncomingResponseHook peer=%s response requestId=%s status=%s\n", p.String(), reqId, status)
	})
	globalParExchange.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		fmt.Printf("RegisterIncomingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	globalParExchange.RegisterBlockSentListener(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
		fmt.Printf("RegisterBlockSentListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	globalParExchange.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		fmt.Printf("RegisterCompletedResponseListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	globalParExchange.RegisterIncomingRequestQueuedHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
		fmt.Printf("RegisterIncomingRequestQueuedHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	globalParExchange.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		fmt.Printf("RegisterNetworkErrorListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	globalParExchange.RegisterOutgoingBlockHook(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		fmt.Printf("RegisterOutgoingBlockHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
	})
	globalParExchange.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
		fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		hookActions.UsePersistenceOption("newLinkSys")
	})
	//uninitialized, is it a bug?
	//globalParExchange.RegisterOutgoingRequestProcessingListener(func(p peer.ID, request graphsync.RequestData, inProgressRequestCount int) {
	//	fmt.Printf("request requestId=%s\n", request.ID().String())
	//})
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	globalParExchange.RegisterPersistenceOption("newLinkSys", newlsys)

	globalParExchange.RegisterReceiverNetworkErrorListener(func(p peer.ID, err error) {
		fmt.Printf("RegisterReceiverNetworkErrorListener error=%s\n", err)
	})
	globalParExchange.RegisterRequestorCancelledListener(func(p peer.ID, request graphsync.RequestData) {
		fmt.Printf("RegisterRequestorCancelledListener request requestId=%s\n", request.ID().String())
	})
	globalParExchange.RegisterRequestUpdatedHook(func(p peer.ID, request graphsync.RequestData, updateRequest graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
		fmt.Printf("RegisterRequestUpdatedHook request requestId=%s\n", request.ID().String())
	})
	// QmTTSVQrNxBvQDXevh3UvToezMw1XQ5hvTMCwpDc8SDnNT
	// Qmf5VLQUwEf4hi8iWqBWC21ws64vWW6mJs9y6tSCLunz5Y
	globalRoot, _ = cid.Parse("Qmf5VLQUwEf4hi8iWqBWC21ws64vWW6mJs9y6tSCLunz5Y")
	for _, addrInfo := range addrInfos {
		globalHost.Peerstore().AddAddr(addrInfo.ID, addrInfo.Addrs[0], peerstore.PermanentAddrTTL)
	}
	bigCarRootCid, _ = cid.Parse("QmSvtt6abwrp3MybYqHHA4BdFjjuLBABXjLEVQKpMUfUU8")
	bigCarParExchange, bigCarAddrInfos = startWithBigCar()
	os.Exit(m.Run())
}

func TestGraphSync(t *testing.T) {
	var responseProgress <-chan graphsync.ResponseProgress
	var errors <-chan error

	globalParExchange.UnregisterPersistenceOption("newLinkSys")
	memds := datastore.NewMapDatastore()
	membs := blockstore.NewBlockstore(dssync.MutexWrap(memds))
	newlsys := storeutil.LinkSystemForBlockstore(membs)
	if err := globalParExchange.RegisterPersistenceOption("newLinkSys", newlsys); err != nil {
		t.Fatal(err)
	}

	// create a selector to traverse the whole tree
	allSelector := selectorparse.CommonSelector_ExploreAllRecursively

	responseProgress, errors = globalParExchange.Request(context.TODO(), globalAddrInfos[0].ID, cidlink.Link{globalRoot}, allSelector)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	for blk := range responseProgress {
		fmt.Printf("path=%s \n", blk.Path.String())
	}

}

func TestUnixfsPathGraphSync(t *testing.T) {
	// data from ipfs
	serverbs, err := loadCarV2Blockstore("./QmREu6imfQ38NgCuSWMX5i9Vj9UATvF2CJfPoRu49p58iz.car")
	if err != nil {
		t.Fatal(err)
	}
	root, _ := cid.Parse("QmREu6imfQ38NgCuSWMX5i9Vj9UATvF2CJfPoRu49p58iz")
	subRoot, _ := cid.Parse("QmTGrEttSZqV82rABfjc88iEGKXHzPvrRrtsH35Eu28AD2")
	sel := unixfsnode.UnixFSPathSelector("1.jpg")
	var s strings.Builder
	dagjson.Encode(sel, &s)
	t.Logf(s.String())
	mainCtx := context.TODO()
	addrInfos, err := startSomeGraphSyncServicesByBlockStore(mainCtx, ServicesNum, 9135, serverbs, false)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := path.Join(os.TempDir(), "gs-unixfs-key")
	ds := datastore.NewMapDatastore()
	bs := blockstore.NewBlockstore(dssync.MutexWrap(ds))

	host, gscli, err := startPraGraphSyncClient(context.TODO(), "/ip4/0.0.0.0/tcp/9140", keyFile, bs)
	if err != nil {
		t.Fatal(err)
	}

	host.Peerstore().AddAddr(addrInfos[0].ID, addrInfos[0].Addrs[0], peerstore.PermanentAddrTTL)

	responseProgress, errors := gscli.Request(mainCtx, addrInfos[0].ID, cidlink.Link{root}, sel)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	for blk := range responseProgress {
		t.Logf("path=%s links=%d\n", blk.Path.String(), blk.Node.Length())
	}

	rdag := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))
	nd, err := rdag.Get(mainCtx, subRoot)
	if err != nil {
		t.Fatal(err)
	}
	file, err := unixfile.NewUnixfsFile(mainCtx, rdag, nd)
	if err != nil {
		t.Fatal(err)
	}
	filePath := path.Join(t.TempDir(), "1.jpg")
	t.Log(filePath)
	err = NodeWriteTo(file, filePath)
	if err != nil {
		t.Fatal(err)
	}
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := ioutil.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := ioutil.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

var DefaultHashFunction = uint64(mh.BLAKE2B_MIN + 31)

func unixFSCidBuilder() (cid.Builder, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize UnixFS CID Builder: %w", err)
	}
	prefix.MhType = DefaultHashFunction
	b := cidutil.InlineBuilder{
		Builder: prefix,
		Limit:   126,
	}
	return b, nil
}

func createCarV2Blockstore(path string) (*carv2bs.ReadWrite, error) {
	b, err := unixFSCidBuilder()
	if err != nil {
		return nil, err
	}

	// placeholder payload needs to be larger than inline CID threshold; 256
	// bytes is a safe value.
	placeholderRoot, err := b.Sum(make([]byte, 256))
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate placeholder root: %w", err)
	}

	bs, err := carv2bs.OpenReadWrite(path, []cid.Cid{placeholderRoot}, carv2bs.UseWholeCIDs(true))
	if err != nil {
		return nil, xerrors.Errorf("failed to create carv2 read/write retrieval: %w", err)
	}

	return bs, nil
}

func loadCarV2Blockstore(path string) (*carv2bs.ReadOnly, error) {
	bs, err := carv2bs.OpenReadOnly(path, carv2bs.UseWholeCIDs(true))
	if err != nil {
		return nil, xerrors.Errorf("failed to create carv2 read retrieval: %w", err)
	}
	return bs, nil
}

func startGraphSyncService(ctx context.Context, listenAddr string, peerkey crypto.PrivKey, bs blockstore.Blockstore, printLog bool) (graphsync.GraphExchange, error) {
	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.DefaultTransports,
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}
	if printLog {
		fmt.Printf("host multiAddrs: %v\n", host.Addrs())
	}

	lsys := storeutil.LinkSystemForBlockstore(bs)

	network := gsnet.NewFromLibp2pHost(host)
	exchange := graphsyncImpl.New(ctx, network, lsys)
	// automatically validate incoming requests for content
	exchange.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		id := request.ID().String()
		root := request.Root().String()
		var b strings.Builder
		err := dagjson.Encode(request.Selector(), &b)
		if err != nil {
			fmt.Printf("RegisterIncomingRequestHook peer=%s request id=%s root=%s err=%v\n", p.String(), id, root, err)
		}
		if printLog {
			fmt.Printf("RegisterIncomingRequestHook peer=%s request id=%s root=%s selecter=%s\n", p.String(), id, root, b.String())
		}
		hookActions.ValidateRequest()
	})
	if printLog {
		exchange.RegisterIncomingBlockHook(func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
			fmt.Printf("RegisterIncomingBlockHook peer=%s block index=%d, size=%d link=%s\n", p.String(), blockData.Index(), blockData.BlockSize(), blockData.Link().String())
		})
		exchange.RegisterIncomingResponseHook(func(p peer.ID, responseData graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
			reqId := responseData.RequestID().String()
			status := responseData.Status().String()
			fmt.Printf("RegisterIncomingResponseHook peer=%s response requestId=%s status=%s\n", p.String(), reqId, status)
		})
		exchange.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
			fmt.Printf("RegisterIncomingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterBlockSentListener(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
			fmt.Printf("RegisterBlockSentListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
			fmt.Printf("RegisterCompletedResponseListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterIncomingRequestQueuedHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
			fmt.Printf("RegisterIncomingRequestQueuedHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
			fmt.Printf("RegisterNetworkErrorListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterOutgoingBlockHook(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
			fmt.Printf("RegisterOutgoingBlockHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterOutgoingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
			fmt.Printf("RegisterOutgoingRequestHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		// not init, is it a bug?
		//globalParExchange.RegisterOutgoingRequestProcessingListener(func(p peer.ID, request graphsync.RequestData, inProgressRequestCount int) {
		//	fmt.Printf("request requestId=%s\n", request.ID().String())
		//})
		exchange.RegisterReceiverNetworkErrorListener(func(p peer.ID, err error) {
			fmt.Printf("RegisterReceiverNetworkErrorListener peer=%s error=%s\n", p.String(), err)
		})
		exchange.RegisterRequestorCancelledListener(func(p peer.ID, request graphsync.RequestData) {
			fmt.Printf("RegisterRequestorCancelledListener peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
		exchange.RegisterRequestUpdatedHook(func(p peer.ID, request graphsync.RequestData, updateRequest graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
			fmt.Printf("RegisterRequestUpdatedHook peer=%s request requestId=%s\n", p.String(), request.ID().String())
		})
	}
	return exchange, nil
}

func startGraphSyncClient(ctx context.Context, listenAddr string, keyFile string, bs blockstore.Blockstore) (host.Host, graphsync.GraphExchange, error) {
	peerkey, err := loadOrInitPeerKey(keyFile)
	if err != nil {
		return nil, nil, err
	}

	cmgr, err := connmgr.NewConnManager(2000, 3000)
	if err != nil {
		return nil, nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.ConnectionManager(cmgr),
		libp2p.Identity(peerkey),
		libp2p.DefaultTransports,
	}

	host, err := libp2p.New(opts...)
	if err != nil {
		return nil, nil, err
	}

	lsys := storeutil.LinkSystemForBlockstore(bs)

	network := gsnet.NewFromLibp2pHost(host)
	exchange := graphsyncImpl.New(ctx, network, lsys)
	return host, exchange, nil
}

func startSomeGraphSyncServices(ctx context.Context, number int, portStart int, printLog bool, path string) ([]peer.AddrInfo, error) {
	bs, err := loadCarV2Blockstore(path)
	if err != nil {
		return nil, err
	}
	return startSomeGraphSyncServicesByBlockStore(ctx, number, portStart, bs, printLog)
}

func startSomeGraphSyncServicesByBlockStore(ctx context.Context, number int, portStart int, bs blockstore.Blockstore, printLog bool) ([]peer.AddrInfo, error) {
	var addrInfos []peer.AddrInfo
	for i := 0; i < number; i++ {
		keyFile := path.Join(os.TempDir(), fmt.Sprintf("globalParExchange-key%d", portStart+i))
		peerkey, err := loadOrInitPeerKey(keyFile)
		if err != nil {
			return nil, err
		}
		peerId, err := peer.IDFromPrivateKey(peerkey)
		if err != nil {
			return nil, err
		}
		maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", portStart+i))
		if err != nil {
			return nil, err
		}
		addrInfos = append(addrInfos, peer.AddrInfo{
			ID:    peerId,
			Addrs: []multiaddr.Multiaddr{maddr},
		})
		go func(i int) {
			_, err := startGraphSyncService(ctx, maddr.String(), peerkey, bs, printLog)
			if err != nil {
				panic(err)
			}
			select {
			case <-ctx.Done():
			}
		}(i)
	}
	return addrInfos, nil
}

func NodeWriteTo(nd files.Node, fpath string) error {
	switch nd := nd.(type) {
	case *files.Symlink:
		return os.Symlink(nd.Target, fpath)
	case files.File:
		f, err := os.Create(fpath)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, nd)
		if err != nil {
			return err
		}
		return nil
	case files.Directory:
		if !ExistDir(fpath) {
			err := os.Mkdir(fpath, 0777)
			if err != nil && os.IsNotExist(err) {
				return err
			}
		}

		entries := nd.Entries()
		for entries.Next() {
			child := filepath.Join(fpath, entries.Name())
			if err := NodeWriteTo(entries.Node(), child); err != nil {
				return err
			}
		}
		return entries.Err()
	default:
		return fmt.Errorf("file type %T at %q is not supported", nd, fpath)
	}
}

func ExistDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}
