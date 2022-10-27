//go:build js && wasm
// +build js,wasm

package main

import (
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"github.com/filedrive-team/go-parallel-graphsync/util/parseselector"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"strings"
	"syscall/js"
)

func unionPathSelector(this js.Value, args []js.Value) interface{} {
	var input []string
	for _, a := range args {
		input = append(input, a.String())
	}
	fmt.Printf("input %s\n", input)
	node, err := util.UnionPathSelector(input)
	if err != nil {
		fmt.Printf("unable to union path to selector %s\n", err)
		return err.Error()
	}
	var s strings.Builder
	dagjson.Encode(node, &s)
	return js.ValueOf(s.String())
}

func parseComplexSelectors(this js.Value, args []js.Value) interface{} {

	fmt.Printf("input %s\n", args[0].String())
	node, err := selectorparse.ParseJSONSelector(args[0].String())
	edge, nedge, err := parseselector.GenerateSelectors(node)
	if err != nil {
		fmt.Printf("unable to generate selectors %s\n", err)
		return err.Error()
	}
	var edges, nedges []string
	for _, n := range edge {
		var edgeStr strings.Builder
		dagjson.Encode(n, &edgeStr)
		edges = append(edges, edgeStr.String())
	}
	for _, n := range nedge {
		var nedgeStr strings.Builder
		dagjson.Encode(n, &nedgeStr)
		nedges = append(nedges, nedgeStr.String())
	}
	selectors := struct {
		edges  []string
		nedges []string
	}{
		edges:  edges,
		nedges: nedges,
	}
	return js.ValueOf(selectors)

}
func main() {
	done := make(chan int, 0)
	js.Global().Set("unionPathSelector", js.FuncOf(unionPathSelector))
	js.Global().Set("parseComplexSelectors", js.FuncOf(parseComplexSelectors))
	<-done
}