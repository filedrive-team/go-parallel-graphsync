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
	node, err := util.UnionPathSelectorWeb(input, false)
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
	parseSelectors, err := parseselector.GenerateSelectors(node)
	if err != nil {
		fmt.Printf("unable to generate selectors %s\n", err)
		return js.ValueOf(err.Error())
	}
	var edges, nedges []interface{}
	for _, n := range parseSelectors {
		if n.Recursive {
			var s strings.Builder
			dagjson.Encode(n.Sel, &s)
			edges = append(edges, s.String())
		} else {
			var s strings.Builder
			dagjson.Encode(n.Sel, &s)
			nedges = append(nedges, s.String())
		}
	}
	return map[string]interface{}{
		"edges":  edges,
		"nedges": nedges,
	}
}
func main() {
	done := make(chan int, 0)
	js.Global().Set("unionPathSelector", js.FuncOf(unionPathSelector))
	js.Global().Set("parseComplexSelectors", js.FuncOf(parseComplexSelectors))
	<-done
}
