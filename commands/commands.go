package commands

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"os"

	"fmt"

	"sync"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

type Command interface {
	Execute()
}

func Get() Command {
	if *globals.CSVFilePath != "" && (*globals.Type == "profile" || *globals.Type == "event") {
		return &uploadEventsProfilesFromCSVCommand{}
	}

	if *globals.MixpanelSecret != "" && *globals.Type == "profile" {
		return &uploadProfilesFromMixpanel{}
	}

	if *globals.MixpanelSecret != "" && *globals.Type == "event" {
		return &uploadEventsFromMixpanel{}
	}

	if globals.MPEventsFilePaths != nil && len(globals.MPEventsFilePaths) > 0 && *globals.Type == "event" {
		return &uploadEventsFromMixpanel{}
	}

	return nil
}

//{
//"status": "success",
//"ctProcessed": 1,
//"ctUnprocessed": []
//}

type CTResponse struct {
	Status      string        `json:"status,omitempty"`
	Processed   int           `json:"processed,omitempty"`
	Unprocessed []interface{} `json:"unprocessed,omitempty"`
	Error       string        `json:"error,omitempty"`
}

var Summary = struct {
	sync.Mutex
	ctProcessed           int64
	ctUnprocessed         int64
	mpParseErrorResponses []string
}{
	ctProcessed:           0,
	ctUnprocessed:         0,
	mpParseErrorResponses: make([]string, 0),
}

func sendData(payload map[string]interface{}, endpoint string) (string, error) {

	if *globals.DryRun {
		json.NewEncoder(os.Stdout).Encode(payload)
		return "", nil
	}

	client := &http.Client{}
	for {
		b := &bytes.Buffer{}
		json.NewEncoder(b).Encode(payload)

		req, err := http.NewRequest("POST", endpoint, b)
		if err != nil {
			log.Println(err)
			return "", err
		}

		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("X-CleverTap-Account-Id", *globals.AccountID)
		req.Header.Add("X-CleverTap-Passcode", *globals.AccountPasscode)

		resp, err := client.Do(req)
		retry := false
		var body []byte
		if err == nil {
			body, _ = ioutil.ReadAll(resp.Body)
		}

		if resp.StatusCode == http.StatusBadRequest && err == nil && *globals.Type == "profile" {
			//{ "status" : "fail" , "error" : "Malformed request" , "code" : 400}
			respFromCT := &CTResponse{}
			ctRespError := json.Unmarshal(body, respFromCT)
			if ctRespError == nil && respFromCT.Error == "Malformed request" {
				retry = true
			}
		}

		if err == nil && resp.StatusCode < 500 && !retry {
			responseText := string(body)
			log.Printf("response body: %v , status code: %v", responseText, resp.StatusCode)
			//{ "status" : "success" , "ctProcessed" : 2 , "ctUnprocessed" : [ ]}
			if resp.StatusCode == http.StatusBadRequest {
				fmt.Println("status 400 for:")
				json.NewEncoder(os.Stdout).Encode(payload)
			}
			if resp.StatusCode == http.StatusOK {
				respFromCT := &CTResponse{}
				ctRespError := json.Unmarshal(body, respFromCT)
				if ctRespError == nil {
					processed := respFromCT.Processed
					unprocessed := len(respFromCT.Unprocessed)
					Summary.Lock()
					Summary.ctProcessed += int64(processed)
					Summary.ctUnprocessed += int64(unprocessed)
					Summary.Unlock()
				}
			}
			resp.Body.Close()
			return responseText, nil
		}

		if err != nil {
			log.Println("Error", err)
			log.Println("retrying after 20 seconds")
		} else {
			//body, _ := ioutil.ReadAll(resp.Body)
			log.Println("response body: ", string(body))
			log.Println("response body: ", "retrying for payload after 20 seconds: ")
			json.NewEncoder(os.Stdout).Encode(payload)
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(20 * time.Second)
	}
}
