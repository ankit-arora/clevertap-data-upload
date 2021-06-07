package commands

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"encoding/json"

	"strconv"

	"time"

	"strings"

	"sync"

	"bufio"

	"reflect"

	"os"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

const (
	mixpanelProfilesExportEP = "https://mixpanel.com/api/2.0/engage/"
	mixpanelEventsExportEP   = "https://data.mixpanel.com/api/2.0/export/"
	maxPropsCount            = 255
)

var restrictedEvents = []string{
	"Notification Sent", "Notification Viewed", "Notification Clicked", "UTM Visited", "App Launched", "App Uninstalled", "Stayed",
}

var propertiesMap = map[string]string{
	"name":          "Name",
	"email":         "Email",
	"gender":        "Gender",
	"facebook_id":   "fbId",
	"timezone":      "Timezone",
	"date_of_birth": "Birthday",
	"phone":         "Phone",
}

type uploadProfilesFromMixpanel struct {
}

func (u *uploadProfilesFromMixpanel) Execute() {
	log.Println("started")
	//ct batch size of 100 for MP
	ctBatchSize = 100
	var wg sync.WaitGroup
	done := make(chan interface{})
	batchAndSendToCTAPI(done, processAPIRecordForUpload(done, mixpanelProfileRecordsGenerator(done)), &wg)
	wg.Wait()
	log.Println("done")
	log.Printf("Profiles Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
}

//{"page": 0,
//"page_size": 1000,
//"results": [{"$distinct_id": 4,
//"$properties": {"$created": "2008-12-12T11:20:47",
//"$email": "example@mixpanel.com",
//"$first_name": "Example",
//"$last_name": "Name",
//"$last_seen": "2008-06-09T23:08:40",}}],
//"session_id": "1234567890-EXAMPL",
//"status": "ok",
//"total": 1}

type profileResult struct {
	DistinctID string                 `json:"$distinct_id,omitempty"`
	Properties map[string]interface{} `json:"$properties,omitempty"`
}

type mixpanelProfileRecordInfo struct {
	Page      int             `json:"page"`
	PageSize  int             `json:"page_size"`
	Results   []profileResult `json:"results,omitempty"`
	SessionID string          `json:"session_id"`
	Status    string          `json:"status"`
	Total     int             `json:"total"`
}

func (p *mixpanelProfileRecordInfo) convertToCTAPIFormat() ([]interface{}, error) {
	records := make([]interface{}, 0)

	for _, r := range p.Results {
		identity := r.DistinctID
		if identity != "" {
			record := make(map[string]interface{})
			record["identity"] = identity
			record["ts"] = time.Now().Unix()
			record["type"] = "profile"
			propertyData := make(map[string]interface{})
			propsCount := 0
			for k, v := range r.Properties {
				if propsCount > maxPropsCount {
					break
				}
				if v == nil {
					continue
				}

				//rt := reflect.TypeOf(v)
				//switch rt.Kind() {
				//case reflect.Slice:
				//	continue
				//case reflect.Array:
				//	continue
				//default:
				//
				//}

				if k == "Email" && propertyData[k] != nil {
					continue
				}

				if strings.HasPrefix(k, "$") {
					k = k[1:]
				}

				if nK, ok := propertiesMap[k]; ok {
					k = nK
				}

				//Date Of Birth
				//Email
				//Phone

				if k == "Date Of Birth" || k == "Phone" {
					continue
				}

				propertyData[k] = v
				propsCount++
			}
			record["profileData"] = propertyData
			records = append(records, record)
		} else {
			log.Printf("Identity not found for record. Skipping: %v", r)
		}
	}
	return records, nil
}

func (p *mixpanelProfileRecordInfo) print() {
	log.Printf("First Result: %v", p.Results[0])
	log.Printf("Results size: %v", len(p.Results))
}

func mixpanelProfileRecordsGenerator(done chan interface{}) <-chan apiUploadRecordInfo {
	mixpanelRecordStream := make(chan apiUploadRecordInfo)
	go func() {
		defer close(mixpanelRecordStream)
		client := &http.Client{Timeout: time.Minute * 1}
		sessionID := ""
		page := "0"
		pageSize := 0
		encodedSecret := base64.StdEncoding.EncodeToString([]byte(*globals.MixpanelSecret))
		for {
			endpoint := mixpanelProfilesExportEP
			if sessionID != "" {
				endpoint += "?session_id=" + sessionID + "&page=" + page
			}
			log.Printf("Fetching profiles data from Mixpanel for page: %v", page)
			req, err := http.NewRequest("GET", endpoint, nil)
			if err != nil {
				log.Fatal(err)
				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}
			}
			req.Header.Add("Authorization", "Basic "+encodedSecret)
			resp, err := client.Do(req)
			if err == nil && resp.StatusCode <= 500 {
				info := &mixpanelProfileRecordInfo{}
				err = json.NewDecoder(resp.Body).Decode(info)
				if err != nil {
					log.Println("Error parsing profiles json response from Mixpanel", err)
					log.Printf("retrying for session_id : %v and page : %v after 20 seconds", sessionID, page)
					ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					time.Sleep(20 * time.Second)
					continue
				}

				ioutil.ReadAll(resp.Body)
				resp.Body.Close()

				select {
				case <-done:
					return
				case mixpanelRecordStream <- info:
				}

				if sessionID == "" {
					pageSize = info.PageSize
					sessionID = info.SessionID
					log.Printf("Mixpanel request page size: %v", pageSize)
					log.Printf("Mixpanel request session id: %v", sessionID)
				}
				if len(info.Results) < pageSize {
					//got less number of results from pageSize. End of response
					break
				}
				//continue with next session id and page
				page = strconv.Itoa(info.Page + 1)
				continue
			}
			if err != nil {
				log.Println("Error while fetching data from Mixpanel: ", err)
				log.Println("retrying after 20 seconds")
			} else {
				body, _ := ioutil.ReadAll(resp.Body)
				log.Println("response body: ", string(body))
				log.Printf("retrying for session_id : %v and page : %v after 20 seconds", sessionID, page)
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(20 * time.Second)
		}
	}()
	return mixpanelRecordStream
}

type uploadEventsFromMixpanel struct {
}

func (u *uploadEventsFromMixpanel) Execute() {
	log.Println("started")
	//ct batch size of 100 for MP
	ctBatchSize = 100
	var wg sync.WaitGroup
	done := make(chan interface{})
	if globals.MPEventsFilePaths != nil && len(globals.MPEventsFilePaths) > 0 {
		batchAndSendToCTAPI(done, processAPIRecordForUpload(done, mixpanelEventRecordsFromFilesGenerator(done)), &wg)
	} else {
		batchAndSendToCTAPI(done, processAPIRecordForUpload(done, mixpanelEventRecordsGenerator(done)), &wg)
	}
	wg.Wait()
	log.Println("done")
	log.Println("---------------------Summary---------------------")
	log.Printf("Events Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
	if len(Summary.mpParseErrorResponses) > 0 {
		log.Println("Mixpanel Events Parse Error Responses:")
		for _, parseErrorResponse := range Summary.mpParseErrorResponses {
			log.Println(parseErrorResponse)
		}
	}

}

type mixpanelEventRecordInfo struct {
	Event      string                 `json:"event,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

func (e *mixpanelEventRecordInfo) convertToCTAPIFormat() ([]interface{}, error) {
	records := make([]interface{}, 0)
	eventName := e.Event
	if eventName == "" {
		log.Printf("Event name missing for record: %v . Skipping", e)
		return records, nil
	}
	identity, ok := e.Properties["distinct_id"]
	if !ok {
		log.Printf("Identity missing for record: %v . Skipping", e)
		return records, nil
	}
	ts, ok := e.Properties["time"]
	if !ok {
		log.Printf("Time stamp missing for record: %v . Skipping", e)
		return records, nil
	}
	isEventRestricted := false
	for _, r := range restrictedEvents {
		if eventName == r {
			isEventRestricted = true
			break
		}
	}
	if isEventRestricted {
		eventName = "_" + eventName
	}
	record := make(map[string]interface{})
	record["identity"] = identity
	record["type"] = "event"
	record["ts"] = ts
	record["evtName"] = eventName
	propertyData := make(map[string]interface{})
	propsCount := 0
	for k, v := range e.Properties {
		if propsCount > maxPropsCount {
			break
		}
		if k == "distinct_id" || k == "time" {
			continue
		}
		if strings.HasPrefix(k, "$") {
			continue
		}
		if v == nil {
			continue
		}
		isNested := false
		valueType := reflect.TypeOf(v)
		switch valueType.Kind() {
		case reflect.Slice:
			isNested = true
			break
		case reflect.Array:
			isNested = true
			break
		default:
		}
		vTemp := ""
		if isNested {
			vArr := v.([]interface{})
			for index, vS := range vArr {
				vsT := reflect.TypeOf(vS)
				if vsT != nil {
					if vsT.Kind() == reflect.String || vsT.Kind() == reflect.Float64 {
						if vsT.Kind() == reflect.String {
							vTemp += vS.(string)
							if index != len(vArr)-1 {
								vTemp += ","
							}
						} else {
							vTemp += fmt.Sprintf("%v", vS)
							if index != len(vArr)-1 {
								vTemp += ","
							}
						}
					}
				}
			}
			propertyData[k] = vTemp
			//log.Printf("nested key: %v , nested value: %v , vArr: %v", k, vTemp, vArr)
		} else {
			propertyData[k] = v
		}
		propsCount++
	}
	record["evtData"] = propertyData
	records = append(records, record)
	return records, nil
}

func (e *mixpanelEventRecordInfo) print() {
	//fmt.Printf("\nresponse: %v", e.response)
}

func mixpanelEventRecordsGenerator(done chan interface{}) <-chan apiUploadRecordInfo {
	mixpanelRecordStream := make(chan apiUploadRecordInfo)
	go func() {
		defer close(mixpanelRecordStream)
		client := &http.Client{Timeout: time.Minute * 240}
		eventsDate := *globals.StartDate
		endDate := *globals.EndDate
		if endDate == "" {
			endDate = time.Now().Local().Format("2006-01-02")
		}
		log.Printf("Fetching events with start date: %v and end date: %v ", eventsDate, endDate)
		encodedSecret := base64.StdEncoding.EncodeToString([]byte(*globals.MixpanelSecret))
		for {
			log.Printf("Fetching events data from Mixpanel for date: %v", eventsDate)
			endpoint := fmt.Sprintf(mixpanelEventsExportEP+"?from_date=%v&to_date=%v", eventsDate, eventsDate)
			req, err := http.NewRequest("GET", endpoint, nil)
			if err != nil {
				log.Fatal(err)
				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}
			}
			req.Header.Add("Authorization", "Basic "+encodedSecret)
			resp, err := client.Do(req)
			if err == nil && resp.StatusCode < 300 {
				scanner := bufio.NewScanner(resp.Body)
				scanner.Split(ScanCRLF)
				for scanner.Scan() {
					s := scanner.Text()
					s = strings.Trim(s, " \n \r")
					info := &mixpanelEventRecordInfo{}
					err = json.Unmarshal([]byte(s), info)
					if err != nil {
						log.Printf("Error parsing event record %v. Skipping", s)
						Summary.mpParseErrorResponses = append(Summary.mpParseErrorResponses, s)
					} else {
						if ts, ok := info.Properties["time"]; ok {
							if *globals.StartTs > 0 && ts.(float64) < *globals.StartTs {
								//log.Printf("start ts: %v , ts: %v", *globals.StartTs, ts.(float64))
								continue
							}
						}
						select {
						case <-done:
							return
						case mixpanelRecordStream <- info:
						}
					}
				}
				if err := scanner.Err(); err != nil {
					log.Fatal(err)
					select {
					case <-done:
						return
					default:
						done <- struct{}{}
						return
					}
				}

				resp.Body.Close()

				if eventsDate == endDate {
					//reached end date
					break
				}
				//continue with next date
				t, _ := time.Parse("2006-01-02", eventsDate)
				t = t.AddDate(0, 0, 1)
				eventsDate = t.Format("2006-01-02")
				continue
			}
			if err != nil {
				log.Println("Error while fetching events data from Mixpanel: ", err)
				log.Printf("retrying after 20 seconds for date: %v", eventsDate)
			} else {
				body, _ := ioutil.ReadAll(resp.Body)
				log.Println("response body: ", string(body))
				log.Printf("retrying after 20 seconds for date: %v", eventsDate)
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(20 * time.Second)
		}
	}()
	return mixpanelRecordStream
}

func mixpanelEventRecordsFromFilesGenerator(done chan interface{}) <-chan apiUploadRecordInfo {
	mixpanelRecordStream := make(chan apiUploadRecordInfo)
	go func() {
		defer close(mixpanelRecordStream)
		for _, mpEventsFilePath := range globals.MPEventsFilePaths {
			log.Printf("Fetching events data from Mixpanel events file: %v", mpEventsFilePath)
			file, err := os.Open(mpEventsFilePath)
			if err != nil {
				log.Fatal(err)
				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}
			}
			scanner := bufio.NewScanner(file)
			scanner.Split(ScanCRLF)
			for scanner.Scan() {
				s := scanner.Text()
				s = strings.Trim(s, " \n \r")
				info := &mixpanelEventRecordInfo{}
				err = json.Unmarshal([]byte(s), info)
				if err != nil {
					log.Printf("Error parsing event record %v. Skipping", s)
					Summary.mpParseErrorResponses = append(Summary.mpParseErrorResponses, s)
				} else {
					if ts, ok := info.Properties["time"]; ok {
						if *globals.StartTs > 0 && ts.(float64) < *globals.StartTs {
							//log.Printf("start ts: %v , ts: %v", *globals.StartTs, ts.(float64))
							continue
						}
					}
					select {
					case <-done:
						file.Close()
						return
					case mixpanelRecordStream <- info:
					}
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
				select {
				case <-done:
					file.Close()
					return
				default:
					done <- struct{}{}
					file.Close()
					return
				}
			}

			file.Close()
		}
	}()
	return mixpanelRecordStream
}
