package util

import (
	"context"
	"fmt"
	pargraphsync "github.com/filedrive-team/go-parallel-graphsync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	textselector "github.com/ipld/go-ipld-selector-text-lite"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	LeafLinks  = "/%v/Hash/Links"
	LeftLinks  = "Links/0/Hash"
	CheckLinks = "/%v/Hash"
)

type ParGSTask struct {
	Gs           pargraphsync.ParallelGraphExchange
	AddedTasks   map[string]struct{}
	StartedTasks map[string]struct{}
	RunningTasks chan Tasks
	DoneTasks    map[string]struct{}
	RootCid      cidlink.Link
	PeerIds      []peer.AddrInfo
}

type Task struct {
	Sel    ipld.Node
	PeerId peer.ID
}
type Tasks struct {
	Tasks []Task
}

//todo implement choose
func (s *ParGSTask) choosePeer() peer.ID {
	return s.PeerIds[0].ID
}
func StartParGraphSyncTask(ctx context.Context, gs pargraphsync.ParallelGraphExchange, root cidlink.Link, peerIds []peer.AddrInfo) {
	var s = ParGSTask{
		Gs:           gs,
		AddedTasks:   make(map[string]struct{}),
		StartedTasks: make(map[string]struct{}),
		RunningTasks: make(chan Tasks, 1),
		DoneTasks:    make(map[string]struct{}),
		RootCid:      root,
		PeerIds:      peerIds,
	}
	task := Task{LeftSelector(""), s.choosePeer()}
	s.RunningTasks <- Tasks{Tasks: []Task{task}}
	s.StartRun(ctx)
}
func (s *ParGSTask) StartRun(ctx context.Context) {
	for {
		select {
		case ta := <-s.RunningTasks:
			var params []pargraphsync.RequestParam
			for _, v := range ta.Tasks {
				params = append(params, pargraphsync.RequestParam{
					PeerId:   v.PeerId,
					Root:     s.RootCid,
					Selector: v.Sel,
				})
			}
			s.run(ctx, params)
		default:
			if !s.divideTasks() {
				s.Close()
				return
			}
		}
	}
}

func (s *ParGSTask) Close() {
	fmt.Println("close")
}
func (s *ParGSTask) CollectTasks(pathMap map[string]int64) {
	for k, v := range pathMap {
		for i := int64(0); i < v; i++ {
			if s.checkNode(k + fmt.Sprintf(CheckLinks, i)) {
				continue
			}
			s.AddedTasks[k+fmt.Sprintf(LeafLinks, i)] = struct{}{}
		}
	}
}

func (s *ParGSTask) divideTasks() bool {
	var paths []string
	for k, v := range s.AddedTasks {
		s.StartedTasks[k] = v
		paths = append(paths, k)
		delete(s.AddedTasks, k)
	}
	if len(paths) == 0 {
		return false
	}
	s.RunningTasks <- dividePath(paths, s.PeerIds)
	return true
}

func (s *ParGSTask) checkNode(path string) bool {

	if _, ok := s.AddedTasks[path]; ok {
		return true
	}
	if _, ok := s.StartedTasks[path]; ok {
		return true
	}
	if _, ok := s.DoneTasks[path]; ok {
		return true
	}
	return false
}

func (s *ParGSTask) run(ctx context.Context, params []pargraphsync.RequestParam) {
	// todo collect task
	responseProgress, errors := s.Gs.RequestMany(ctx, params)
	go func() {
		select {
		case err := <-errors:
			if err != nil {
				return
			}
		}
	}()
	for blk := range responseProgress {
		s.DoneTasks[blk.Path.String()] = struct{}{}
	}
}

func dividePath(paths []string, peerIds []peer.AddrInfo) Tasks {
	ave := len(paths)/len(peerIds) + 1
	var selPaths [][]string
	var start, end = 0, 0
	num := len(peerIds)
	if num > len(paths) {
		num = len(paths)
	}
	for i := 0; i < num; i++ {
		end = start + ave
		if i == num-1 {
			end = len(paths)
		}
		selPaths = append(selPaths, paths[start:end])
		start = end
	}
	var tasks []Task
	for i, v := range selPaths {
		sel, err := UnionSelector(v)
		if err != nil {
			continue
		}
		tasks = append(tasks, Task{
			Sel:    sel,
			PeerId: peerIds[i].ID,
		})

	}
	return Tasks{tasks}
}
func LeftSelector(path string) ipld.Node {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selSpec, _ := textselector.SelectorSpecFromPath(LeftLinks, false, ssb.ExploreRecursiveEdge())
	fromPath, _ := textselector.SelectorSpecFromPath(textselector.Expression(path), false, ssb.ExploreRecursive(selector.RecursionLimitNone(), selSpec))
	return fromPath.Node()
}
