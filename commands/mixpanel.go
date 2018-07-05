package commands

import (
	"encoding/base64"
	"io/ioutil"
	"log"
	"net/http"

	"encoding/json"

	"strconv"

	"time"

	"strings"

	"sync"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

const (
	MIXPANEL_PROFILES_EXPORT_EP = "https://mixpanel.com/api/2.0/engage/"
	MIXPANEL_EVENTS_EXPORT_EP   = "https://data.mixpanel.com/api/2.0/export/"
	MAX_PROPS_COUNT             = 255
)

var RESTRICTED_EVENTS = []string{
	"Notification Sent", "Notification Viewed", "Notification Clicked", "UTM Visited", "App Launched", "App Uninstalled", "Stayed",
}

var PROPERTIES_MAP = map[string]string{
	"name":          "Name",
	"email":         "Email",
	"gender":        "Gender",
	"facebook_id":   "fbId",
	"timezone":      "Timezone",
	"date_of_birth": "Birthday",
}

type uploadProfilesFromMixpanel struct {
}

func (u *uploadProfilesFromMixpanel) Execute() {
	log.Println("started")
	//ct batch size of 100 for MP
	ctBatchSize = 100
	var wg sync.WaitGroup
	done := make(chan interface{})
	batchAndSend(done, processMixpanelRecordForUpload(done, mixpanelProfileRecordsGenerator(done)), &wg)
	wg.Wait()
	log.Println("done")
}

type mixpanelRecordInfo interface {
	convertToCT() ([]interface{}, error)
	print()
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
	DistinctId string                 `json:"$distinct_id,omitempty"`
	Properties map[string]interface{} `json:"$properties,omitempty"`
}

type mixpanelProfileRecordInfo struct {
	Page      int             `json:"page"`
	PageSize  int             `json:"page_size"`
	Results   []profileResult `json:"results,omitempty"`
	SessionId string          `json:"session_id"`
	Status    string          `json:"status"`
	Total     int             `json:"total"`
}

func (p *mixpanelProfileRecordInfo) convertToCT() ([]interface{}, error) {
	records := make([]interface{}, 0)

	for _, r := range p.Results {
		identity := r.DistinctId
		if identity != "" {
			record := make(map[string]interface{})
			record["identity"] = identity
			record["ts"] = time.Now().Unix()
			record["type"] = "profile"
			propertyData := make(map[string]interface{})
			propsCount := 0
			for k, v := range r.Properties {
				if propsCount > MAX_PROPS_COUNT {
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

				if strings.HasPrefix(k, "$") {
					k = k[1:]
				}

				if nK, ok := PROPERTIES_MAP["k"]; ok {
					k = nK
				}
				propertyData[k] = v
				record["profileData"] = propertyData
				propsCount++
			}
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

func mixpanelProfileRecordsGenerator(done chan interface{}) <-chan mixpanelRecordInfo {
	mixpanelRecordStream := make(chan mixpanelRecordInfo)
	go func() {
		defer close(mixpanelRecordStream)
		client := &http.Client{Timeout: time.Minute * 1}
		sessionId := ""
		page := "0"
		pageSize := 0
		encodedSecret := base64.StdEncoding.EncodeToString([]byte(*globals.MixpanelSecret))
		for {
			endpoint := MIXPANEL_PROFILES_EXPORT_EP
			if sessionId != "" {
				endpoint += "?session_id=" + sessionId + "&page=" + page
			}
			log.Printf("Fetching data from Mixpanel for page: %v", page)
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
					log.Println("Error parsing json response from Mixpanel", err)
					select {
					case <-done:
						return
					default:
						done <- struct{}{}
						return
					}
				}

				ioutil.ReadAll(resp.Body)
				resp.Body.Close()

				select {
				case <-done:
					return
				case mixpanelRecordStream <- info:
				}

				if sessionId == "" {
					pageSize = info.PageSize
					sessionId = info.SessionId
					log.Printf("Mixpanel request page size: %v", pageSize)
					log.Printf("Mixpanel request session id: %v", sessionId)
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
				log.Printf("retrying for session_id : %v and page : %v after 20 seconds", sessionId, page)
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(20 * time.Second)
		}
	}()
	return mixpanelRecordStream
}

func processMixpanelRecordForUpload(done chan interface{}, mixpanelRecordStream <-chan mixpanelRecordInfo) <-chan interface{} {
	recordStream := make(chan interface{})
	go func() {
		defer close(recordStream)
		for mpRecordInfo := range mixpanelRecordStream {
			ctRecords, err := mpRecordInfo.convertToCT()
			if err != nil {
				log.Println("Error converting Micpanel ctRecords to Clevertap", err)
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
				case recordStream <- ctRecord:
				}
			}
		}
	}()
	return recordStream
}

type uploadEventsFromMixpanel struct {
}

func (u *uploadEventsFromMixpanel) Execute() {
	log.Println("started")
	//ct batch size of 100 for MP
	//ctBatchSize = 100
	//var wg sync.WaitGroup
	done := make(chan interface{})
	//batchAndSend(done, processMixpanelRecordForUpload(done, mixpanelProfileRecordsGenerator(done)), &wg)
	//wg.Wait()

	mixpanelEventRecordsGenerator(done)

	log.Println("done")
}

func mixpanelEventRecordsGenerator(done chan interface{}) <-chan mixpanelRecordInfo {
	mixpanelRecordStream := make(chan mixpanelRecordInfo)
	go func() {
		defer close(mixpanelRecordStream)
		//client := &http.Client{Timeout: time.Minute * 4}
		//startDate := *globals.StartDate
		//endDate := time.Now().Local().Format("2006-01-02")
		//encodedSecret := base64.StdEncoding.EncodeToString([]byte(*globals.MixpanelSecret))
		//for {
		//	log.Printf("Fetching events data from Mixpanel for page: %v", startDate)
		//	endpoint := fmt.Sprintf(MIXPANEL_EVENTS_EXPORT_EP+"?from_date=%v&to_date=%v", startDate, startDate)
		//	req, err := http.NewRequest("GET", endpoint, nil)
		//	if err != nil {
		//		log.Fatal(err)
		//		select {
		//		case <-done:
		//			return
		//		default:
		//			done <- struct{}{}
		//			return
		//		}
		//	}
		//	req.Header.Add("Authorization", "Basic "+encodedSecret)
		//	resp, err := client.Do(req)
		//	if err == nil && resp.StatusCode <= 500 {
		//
		//		if err != nil {
		//			log.Println("Error parsing json response from Mixpanel", err)
		//			select {
		//			case <-done:
		//				return
		//			default:
		//				done <- struct{}{}
		//				return
		//			}
		//		}
		//
		//		ioutil.ReadAll(resp.Body)
		//		resp.Body.Close()
		//
		//		select {
		//		case <-done:
		//			return
		//		case mixpanelRecordStream <- info:
		//		}
		//
		//		if sessionId == "" {
		//			pageSize = info.PageSize
		//			sessionId = info.SessionId
		//			log.Printf("Mixpanel request page size: %v", pageSize)
		//			log.Printf("Mixpanel request session id: %v", sessionId)
		//		}
		//		if len(info.Results) < pageSize {
		//			//got less number of results from pageSize. End of response
		//			break
		//		}
		//		//continue with next session id and page
		//		page = strconv.Itoa(info.Page + 1)
		//		continue
		//	}
		//	if err != nil {
		//		log.Println("Error while fetching data from Mixpanel: ", err)
		//		log.Println("retrying after 20 seconds")
		//	} else {
		//		body, _ := ioutil.ReadAll(resp.Body)
		//		log.Println("response body: ", string(body))
		//		log.Printf("retrying for session_id : %v and page : %v after 20 seconds", sessionId, page)
		//	}
		//	if resp != nil {
		//		resp.Body.Close()
		//	}
		//	time.Sleep(20 * time.Second)
		//	if startDate == endDate {
		//		break
		//	}
		//}
	}()
	return mixpanelRecordStream
}
