package globals

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

var CSVFilePath *string
var SchemaFilePath *string
var MixpanelSecret *string
var StartDate *string
var EndDate *string
var AccountID *string
var AccountPasscode *string
var EvtName *string
var Type *string
var Region *string
var DryRun *bool
var StartTs *float64

//var AutoConvert *bool

type arrayFlags []string

func (i *arrayFlags) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var MPEventsFilePaths arrayFlags

func Init() bool {
	flag.Var(&MPEventsFilePaths, "mixpanelEventsFile", "Absolute path to the MixPanel events file")
	CSVFilePath = flag.String("csv", "", "Absolute path to the csv file")
	SchemaFilePath = flag.String("schema", "", "Absolute path to the schema file")
	MixpanelSecret = flag.String("mixpanelSecret", "", "Mixpanel API secret key")
	StartDate = flag.String("startDate", "", "Start date for exporting events from Mixpanel "+
		"<yyyy-mm--dd>")
	EndDate = flag.String("endDate", "", "End date for exporting events from Mixpanel "+
		"<yyyy-mm--dd>")
	StartTs = flag.Float64("startTs", 0, "Start timestamp for events upload")
	AccountID = flag.String("id", "", "CleverTap Account ID")
	AccountPasscode = flag.String("p", "", "CleverTap Account Passcode")
	EvtName = flag.String("evtName", "", "Event name")
	Type = flag.String("t", "profile", "The type of data, either profile or event, defaults to profile")
	Region = flag.String("r", "eu", "The account region, either eu or in, defaults to eu")
	DryRun = flag.Bool("dryrun", false, "Do a dry run, process records but do not upload")
	//AutoConvert = flag.Bool("autoConvert", false, "automatically covert property value type to number for number entries")
	flag.Parse()
	if (*CSVFilePath == "" && *MixpanelSecret == "" && MPEventsFilePaths == nil) || *AccountID == "" || *AccountPasscode == "" {
		log.Println("Mixpanel secret or CSV file path or Mixpanel events file path, account id, and passcode are mandatory")
		return false
	}
	if *CSVFilePath != "" && *MixpanelSecret != "" {
		log.Println("Both Mixpanel secret and CSV file path detected. Only one data source is allowed")
		return false
	}
	if *Type != "profile" && *Type != "event" {
		log.Println("Type can be either profile or event")
		return false
	}
	if *CSVFilePath != "" && *EvtName == "" && *Type == "event" {
		log.Println("Event name is mandatory for event csv uploads")
		return false
	}
	if *MixpanelSecret != "" && *Type == "event" && *StartDate == "" {
		log.Println("Start date is mandatory when exporting events from Mixpanel. Format: <yyyy-mm-dd>")
		return false
	}
	if *MixpanelSecret != "" && *Type == "event" && *StartDate != "" {
		//check start date format
		_, err := time.Parse("2006-01-02", *StartDate)
		if err != nil {
			log.Println("Start date is not in correct format. Format: <yyyy-mm-dd>")
			return false
		}
	}
	if *MixpanelSecret != "" && *Type == "event" && *EndDate != "" {
		//check end date format
		_, err := time.Parse("2006-01-02", *EndDate)
		if err != nil {
			log.Println("End date is not in correct format. Format: <yyyy-mm-dd>")
			return false
		}
	}
	if *EndDate != "" && *StartDate != "" {
		//start date should be less than or equal to end date
		s, _ := time.Parse("2006-01-02", *StartDate)
		e, _ := time.Parse("2006-01-02", *EndDate)
		if s.After(e) {
			log.Println("Start date cannot be after End date")
			return false
		}
	}
	if MPEventsFilePaths != nil && len(MPEventsFilePaths) > 0 && *Type != "event" {
		log.Println("Mixpanel events file path is supported only with events")
		return false
	}
	if *Region != "eu" && *Region != "in" {
		log.Println("Region can be either eu or in")
		return false
	}
	return true
}

var Schema map[string]string

func ParseSchema(file *os.File) bool {
	/**
	{
		"key": "Float",
		"key 1": "Integer",
		"key 2": "Number"
	}
	*/
	err := json.NewDecoder(file).Decode(&Schema)
	if err != nil {
		log.Println(err)
		log.Println("Unable to parse schema file")
		return false
	}
	return true
}
