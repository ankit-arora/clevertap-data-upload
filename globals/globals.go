package globals

import (
	"flag"
	"log"
)

var CSVFilePath *string
var AccountID *string
var AccountPasscode *string
var EvtName *string
var Type *string
var DryRun *bool
var AutoConvert *bool

func Init() bool {
	CSVFilePath = flag.String("csv", "", "Absolute path to the csv file")
	AccountID = flag.String("id", "", "CleverTap Account ID")
	AccountPasscode = flag.String("p", "", "CleverTap Account Passcode")
	EvtName = flag.String("evtName", "", "Event name")
	Type = flag.String("t", "profile", "The type of data, either profile or event, defaults to profile")
	DryRun = flag.Bool("dryrun", false, "Do a dry run, process records but do not upload")
	//AutoConvert = flag.Bool("autoConvert", false, "automatically covert property value type to number for number entries")
	flag.Parse()
	if *CSVFilePath == "" || *AccountID == "" || *AccountPasscode == "" {
		log.Println("CSV file path, accoun id, and passcode are mandatory")
		return false
	}
	if *Type != "profile" && *Type != "event" {
		log.Println("type can be either profile or event")
		return false
	}
	if *EvtName == "" && *Type == "event" {
		log.Println("event name is mandatory for event uploads")
		return false
	}

	return true
}
