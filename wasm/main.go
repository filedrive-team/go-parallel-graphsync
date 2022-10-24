//go:build js && wasm
// +build js,wasm

package main

import (
	"fmt"
	"github.com/filedrive-team/go-parallel-graphsync/util"
	"syscall/js"
)

func unionPathSelector() js.Func {
	jsonFunc := js.FuncOf(func(this js.Value, args []js.Value) interface{} {
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
		return node
	})
	return jsonFunc
}

func main() {
	fmt.Println("Go Web Assembly")
	js.Global().Set("unionPathSelector", unionPathSelector())
}
