package main

import (
	"log"
	"os"

	"github.com/ankit-arora/clevertap-data-upload/commands"
	"github.com/ankit-arora/clevertap-data-upload/globals"
)

func main() {
	if !globals.Init() {
		return
	}
	if *globals.SchemaFilePath != "" {
		//read schema file
		file, err := os.Open(*globals.SchemaFilePath)
		if err != nil {
			log.Fatal(err)
			return
		}
		if !globals.ParseSchema(file) {
			file.Close()
			return
		}
		file.Close()
	}
	commands.Get().Execute()
}
