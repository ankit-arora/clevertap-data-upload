package main

import (
	"github.com/ankit-arora/clevertap-csv-upload/commands"
	"github.com/ankit-arora/clevertap-csv-upload/globals"
)

func main() {
	if !globals.Init() {
		return
	}
	commands.Get().Execute()
}
