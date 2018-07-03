package commands

import (
	"encoding/base64"
	"io/ioutil"
	"log"
	"net/http"

	"encoding/json"

	"strconv"

	"time"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

const MIXPANEL_PROFILES_EXPORT_EP = "https://mixpanel.com/api/2.0/engage/"

type uploadProfilesFromMixpanel struct {
}

func (u *uploadProfilesFromMixpanel) Execute() {
	log.Println("started")

	//done := make(chan interface{})
	//
	//var wg sync.WaitGroup
	//batchAndSend(done, processCSVLineForUpload(done, csvLineGenerator(done)), &wg)
	//wg.Wait()

	done := make(chan interface{})
	mixpanelRecordStream := mixpanelProfileRecordsGenerator(done)
	for info := range mixpanelRecordStream {
		info.print()
	}

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

	return nil, nil
}

func (p *mixpanelProfileRecordInfo) print() {
	log.Printf("First Result: %v", p.Results[0])
	log.Printf("Results size : %v", len(p.Results))
}

func mixpanelProfileRecordsGenerator(done chan interface{}) <-chan mixpanelRecordInfo {
	mixpanelRecordStream := make(chan mixpanelRecordInfo)
	go func() {
		defer close(mixpanelRecordStream)
		client := &http.Client{}
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
		for recordInfo := range mixpanelRecordStream {
			records, err := recordInfo.convertToCT()
			if err != nil {
				log.Println("Error converting Micpanel records to Clevertap", err)
				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}
			}
			for record := range records {
				select {
				case <-done:
					return
				case recordStream <- record:
				}
			}
		}
	}()
	return recordStream
}
