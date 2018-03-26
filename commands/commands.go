package commands

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/ankit-arora/clevertap-csv-upload/globals"
	"os"
)

type Command interface {
	Execute()
}

func Get() Command {
	if *globals.Type == "profile" || *globals.Type == "event" {
		return &uploadEventsProfilesCommand{}
	}
	return nil
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
		if err == nil && resp.StatusCode == 200 {
			body, _ := ioutil.ReadAll(resp.Body)
			responseText := string(body)
			log.Println("response body: ", responseText)
			//{ "status" : "success" , "processed" : 2 , "unprocessed" : [ ]}
			resp.Body.Close()
			return responseText, nil
		}

		if err != nil {
			log.Println("Error", err)
		} else {
			body, _ := ioutil.ReadAll(resp.Body)
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
