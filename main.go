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
			log.Println("Error in parsing schema file")
			log.Println(err)
			return
		}
		if !globals.ParseSchema(file) {
			file.Close()
			return
		}
		file.Close()
	}
	if globals.FEvents != nil && len(globals.FEvents) > 0 {
		globals.InitFilterEventsSet()
	}
	commands.Get().Execute()
}
