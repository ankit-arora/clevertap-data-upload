package main

import (
	"github.com/ankit-arora/clevertap-data-upload/commands"
	"github.com/ankit-arora/clevertap-data-upload/globals"
)

func main() {
	if !globals.Init() {
		return
	}
	commands.Get().Execute()
}
