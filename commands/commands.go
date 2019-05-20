package commands

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"os"

	"fmt"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

// Command ...
type Command interface {
	Execute()
}

type apiUploadRecordInfo interface {
	convertToCTAPIFormat() ([]interface{}, error)
	print()
}

func processAPIRecordForUpload(done chan interface{}, inputRecordStream <-chan apiUploadRecordInfo) <-chan interface{} {
	convertedRecordStream := make(chan interface{})
	go func() {
		defer close(convertedRecordStream)
		for mpRecordInfo := range inputRecordStream {
			ctRecords, err := mpRecordInfo.convertToCTAPIFormat()
			if err != nil {
				log.Println("Error converting API Records to Clevertap", err)
				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}
			}
			for _, ctRecord := range ctRecords {
				select {
				case <-done:
					return
				case convertedRecordStream <- ctRecord:
				}
			}
		}
	}()
	return convertedRecordStream
}

func processSDKRecordForUpload(done chan interface{}, inputRecordStream <-chan sdkUploadRecordInfo) <-chan []map[string]interface{} {
	convertedRecordStream := make(chan []map[string]interface{})
	go func() {
		defer close(convertedRecordStream)
		for mpRecordInfo := range inputRecordStream {
			ctRecords, err := mpRecordInfo.convertToCTSDKFormat()
			if err != nil {
				log.Println("Error converting SDK Records to Clevertap", err)
				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}
			}

			if ctRecords != nil {
				select {
				case <-done:
					return
				case convertedRecordStream <- ctRecords:
				}
			}
		}
	}()
	return convertedRecordStream
}

type sdkUploadRecordInfo interface {
	convertToCTSDKFormat() ([]map[string]interface{}, error)
	print()
}

// Get ...
func Get() Command {
	if *globals.ImportService == "leanplumToS3" || *globals.ImportService == "leanplumS3ToCT" ||
		*globals.ImportService == "leanplumToS3Throttled" {
		return &uploadRecordsFromLeanplum{}
	}

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

	if *globals.ImportService == "mparticle" {
		return &uploadEventsFromMParticle{}
	}

	return nil
}

//{
//"status": "success",
//"ctProcessed": 1,
//"ctUnprocessed": []
//}

// CTResponse ...
type CTResponse struct {
	Status      string        `json:"status,omitempty"`
	Processed   int           `json:"processed,omitempty"`
	Unprocessed []interface{} `json:"unprocessed,omitempty"`
	Error       string        `json:"error,omitempty"`
}

// Summary ...
var Summary = struct {
	sync.Mutex
	ctProcessed           int64
	ctUnprocessed         int64
	sessionsProcessed     int64
	mpParseErrorResponses []string
}{
	ctProcessed:           0,
	ctUnprocessed:         0,
	sessionsProcessed:     0,
	mpParseErrorResponses: make([]string, 0),
}

const (
	MaxIdleConnections int = 20
	RequestTimeout     int = 1
)

func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: MaxIdleConnections,
		},
		Timeout: time.Duration(RequestTimeout) * time.Minute,
	}

	return client
}

var ctHTTPClient = createHTTPClient()

func sendDataToCTAPI(payload map[string]interface{}, endpoint string) (string, error) {

	if *globals.DryRun {
		json.NewEncoder(os.Stdout).Encode(payload)
		return "", nil
	}

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

		resp, err := ctHTTPClient.Do(req)
		retry := false
		var body []byte
		if err == nil {
			body, _ = ioutil.ReadAll(resp.Body)
		}

		if err == nil && resp.StatusCode == http.StatusBadRequest && *globals.Type == "profile" {
			//{ "status" : "fail" , "error" : "Malformed request" , "code" : 400}
			respFromCT := &CTResponse{}
			ctRespError := json.Unmarshal(body, respFromCT)
			if ctRespError == nil && respFromCT.Error == "Malformed request" {
				retry = true
			}
		}

		if err == nil && resp.StatusCode < 500 && !retry {
			responseText := string(body)
			log.Printf("API response body: %v , status code: %v", responseText, resp.StatusCode)
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
			if resp != nil {
				ioutil.ReadAll(resp.Body)
			}
			log.Println("Error", err)
			log.Println("retrying after 5 seconds")
		} else {
			//body, _ := ioutil.ReadAll(resp.Body)
			log.Println("response body: ", string(body))
			log.Println("response body: ", "retrying for payload after 5 seconds: ")
			json.NewEncoder(os.Stdout).Encode(payload)
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(5 * time.Second)
	}
}

func batchAndSendToCTAPI(done <-chan interface{}, recordStream <-chan interface{}, wg *sync.WaitGroup) {
	for i := 0; i < apiConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			region := ""
			if *globals.Region == "in" {
				region = "in."
			}
			var dataSlice []interface{}
			for e := range recordStream {
				select {
				case <-done:
					return
				default:
					dataSlice = append(dataSlice, e)
					if len(dataSlice) == ctBatchSize {
						p := make(map[string]interface{})
						p["d"] = dataSlice
						sendDataToCTAPI(p, "https://"+region+uploadEndpoint)
						dataSlice = nil
					}
				}
			}
			if len(dataSlice) > 0 {
				select {
				case <-done:
					return
				default:
					p := make(map[string]interface{})
					p["d"] = dataSlice
					sendDataToCTAPI(p, "https://"+region+uploadEndpoint)
					dataSlice = nil
				}
			}
		}()
	}
}

func sendDataToCTSDK(payload []map[string]interface{}, endpoint string) (string, error) {

	if *globals.DryRun {
		json.NewEncoder(os.Stdout).Encode(payload)
		return "", nil
	}

	for {
		b := &bytes.Buffer{}
		json.NewEncoder(b).Encode(payload)

		req, err := http.NewRequest("POST", endpoint, b)
		if err != nil {
			log.Println(err)
			return "", err
		}

		resp, err := ctHTTPClient.Do(req)

		var body []byte
		if err == nil {
			body, _ = ioutil.ReadAll(resp.Body)
		}

		if err == nil && resp.StatusCode < 500 {
			responseText := string(body)
			//log.Printf("SDK response body: %v , status code: %v", responseText, resp.StatusCode)
			resp.Body.Close()
			return responseText, nil
		}

		if err != nil {
			if resp != nil {
				ioutil.ReadAll(resp.Body)
			}
			log.Println("Error", err)
			log.Println("retrying after 5 seconds")
		} else {
			//status code >= 500
			log.Println("response body: ", string(body))
			log.Println("response body: ", "retrying for payload after 5 seconds: ")
			json.NewEncoder(os.Stdout).Encode(payload)
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(5 * time.Second)
	}
}

func sendToCTSDK(endpoint string, done <-chan interface{}, recordStream <-chan []map[string]interface{}, wg *sync.WaitGroup) {
	for i := 0; i < sdkConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//region := ""
			//if *globals.Region == "in" {
			//	region = "in."
			//}
			for e := range recordStream {
				select {
				case <-done:
					return
				default:
					sendDataToCTSDK(e, endpoint)
				}
			}
		}()
	}
}
