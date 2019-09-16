package commands

import (
	"bufio"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankit-arora/clevertap-data-upload/globals"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

type s3CopyEntryInfo struct {
	DestFile   string `json:"destFile"`
	SourceFile string `json:"sourceFile"`
}

type s3CopyStatusInfo struct {
	Success []s3CopyEntryInfo `json:"success"`
}

type jobInfo struct {
	JobID        string           `json:"jobId,omitempty"`
	Success      bool             `json:"success,omitempty"`
	State        string           `json:"state,omitempty"`
	Files        []string         `json:"files,omitempty"`
	S3CopyStatus s3CopyStatusInfo `json:"s3CopyStatus"`
}

type jobResponse struct {
	Res []jobInfo `json:"response,omitempty"`
}

type ExperimentInfo struct {
	Id        int64 `json:"id,omitempty"`
	VariantId int64 `json:"variantId,omitempty"`
	Impressed bool  `json:"impressed,omitempty"`
}

type EventInfo struct {
	EventId               int64                  `json:"eventId,omitempty"`
	Value                 float64                `json:"value,omitempty"`
	Info                  string                 `json:"info,omitempty"`
	Time                  float64                `json:"time,omitempty"`
	Name                  string                 `json:"name,omitempty"`
	TimeUntilFirstForUser float64                `json:"timeUntilFirstForUser,omitempty"`
	Parameters            map[string]interface{} `json:"parameters,omitempty"`
}

type StateInfo struct {
	StateId               int                    `json:"stateId,omitempty"`
	Info                  string                 `json:"info,omitempty"`
	Time                  float64                `json:"time,omitempty"`
	Duration              float64                `json:"duration,omitempty"`
	Name                  string                 `json:"name,omitempty"`
	TimeUntilFirstForUser float64                `json:"timeUntilFirstForUser,omitempty"`
	Parameters            map[string]interface{} `json:"parameters,omitempty"`
	Events                []EventInfo            `json:"events,omitempty"`
}

type leanplumRecordInfo struct {
	IsSession             bool                   `json:"isSession,omitempty"`
	AppVersion            string                 `json:"appVersion,omitempty"`
	Country               string                 `json:"country,omitempty"`
	TimeZone              string                 `json:"timezone,omitempty"`
	Region                string                 `json:"region,omitempty"`
	City                  string                 `json:"city,omitempty"`
	Locale                string                 `json:"locale,omitempty"`
	DeviceModel           string                 `json:"deviceModel,omitempty"`
	PriorEvents           int64                  `json:"priorEvents,omitempty"`
	SystemName            string                 `json:"systemName,omitempty"`
	SystemVersion         string                 `json:"systemVersion,omitempty"`
	PriorStates           int64                  `json:"priorStates, omitempty"`
	Time                  float64                `json:"time, omitempty"`
	DeviceId              string                 `json:"deviceId,omitempty"`
	FirstRun              float64                `json:"firstRun,omitempty"`
	SourcePublisherId     string                 `json:"sourcePublisherId,omitempty"`
	SourcePublisher       string                 `json:"sourcePublisher,omitempty"`
	SourceSubPublisher    string                 `json:"sourceSubPublisher,omitempty"`
	SourceSite            string                 `json:"sourceSite,omitempty"`
	SourceCampaign        string                 `json:"sourceCampaign,omitempty"`
	SourceAdGroup         string                 `json:"sourceAdGroup,omitempty"`
	SourceAd              string                 `json:"sourceAd,omitempty"`
	UserId                string                 `json:"userId,omitempty"`
	Client                string                 `json:"client,omitempty"`
	BrowserName           string                 `json:"browserName,omitempty"`
	BrowserVersion        string                 `json:"browserVersion,omitempty"`
	SdkVersion            string                 `json:"sdkVersion,omitempty"`
	SessionId             string                 `json:"sessionId,omitempty"`
	Lat                   string                 `json:"lat,omitempty"`
	Lon                   string                 `json:"lon,omitempty"`
	Duration              float64                `json:"duration,omitempty"`
	PriorTimeSpentInApp   float64                `json:"priorTimeSpentInApp,omitempty"`
	TimezoneOffsetSeconds int32                  `json:"timezoneOffsetSeconds,omitempty"`
	PriorSessions         int64                  `json:"priorSessions,omitempty"`
	UserBucket            int32                  `json:"userBucket,omitempty"`
	IsDeveloper           bool                   `json:"isDeveloper,omitempty"`
	Experiments           []ExperimentInfo       `json:"experiments,omitempty"`
	States                []StateInfo            `json:"states,omitempty"`
	UserAttributes        map[string]interface{} `json:"userAttributes,omitempty"`
}

func (l *leanplumRecordInfo) convertToCTAPIFormat() ([]interface{}, error) {
	records := make([]interface{}, 0)
	identity := l.UserId
	objectID := l.getObjectID()
	if identity == "" && objectID == "" {
		return records, nil
	}
	profileRecord := make(map[string]interface{})

	if l.UserAttributes != nil {
		profileRecord["type"] = "profile"
		profileRecord["ts"] = time.Now().Unix()
		profileData := make(map[string]interface{})
		for key, val := range l.UserAttributes {
			profileData[key] = val
		}
		if objectID != "" {
			profileRecord["objectId"] = objectID
			if identity != "" {
				profileData["identity"] = identity
			}
		} else {
			profileRecord["identity"] = identity
		}
		profileRecord["profileData"] = profileData
		records = append(records, profileRecord)
	}

	for i := 0; i < len(l.States); i++ {
		for j := 0; j < len(l.States[i].Events); j++ {
			eventRecord := make(map[string]interface{})
			eventRecord["type"] = "event"
			if l.States[i].Events[j].Parameters != nil {
				eventRecord["evtData"] = l.States[i].Events[j].Parameters
			}
			eventRecord["ts"] = int(l.States[i].Events[j].Time)
			eventRecord["evtName"] = l.States[i].Events[j].Name
			if objectID != "" {
				eventRecord["objectId"] = objectID
			} else {
				eventRecord["identity"] = identity
			}
			records = append(records, eventRecord)
		}
	}

	return records, nil
}

func (l *leanplumRecordInfo) getObjectID() string {
	objectID := ""
	if l.UserAttributes != nil {
		systemName := l.SystemName
		adID, ok := l.UserAttributes["adid"]
		if ok && systemName == "Android OS" {
			adIDStr := adID.(string)
			if adIDStr != "" {
				objectID = "__g" + strings.Replace(adIDStr, "-", "", -1)
			}
		} else {
			if ok && systemName != "" {
				log.Printf("Unknown system name: %v", systemName)
			}
			adID, ok = l.UserAttributes["IDFA"]
			if ok && (systemName == "iOS" || systemName == "iPhone OS") {
				adIDStr := adID.(string)
				if adIDStr != "" {
					objectID = "-g" + strings.ToLower(strings.Replace(adIDStr, "-", "", -1))
				}
			} else {
				if ok && systemName != "" {
					log.Printf("Unknown system name: %v", systemName)
				}
			}
		}
	}
	return objectID
}

/*
[{
		"type": "meta",
		"tk": "<token>",
		"g": "__g63a14d2f600d42fa9346def003f620c19645ankit1",
		"id": "<account id>",
		"af": {
			"Version Name": "1.0.6",
			"App Version": "1.0.6",
			"SDK Version": 30309,
			"Model": "Redmi Note 4",
			"Make": "Others",
			"OS Version": "7.0",
			"wdt": 2.57,
			"hgt": 4.84
		}
	},
	{
		"data": {
			"id": "",
			"action": "register",
			"type": "gcm"
		},
		"pg": 1,
		"type": "data"
	}
]
*/

func (l *leanplumRecordInfo) convertToCTSDKFormat() ([]map[string]interface{}, error) {
	objectID := l.getObjectID()
	if objectID == "" || l.AppVersion == "" || l.DeviceModel == "" || l.SystemVersion == "" {
		return nil, nil
	}

	records := make([]map[string]interface{}, 0)

	//meta record
	metaRecord := make(map[string]interface{})
	metaRecord["type"] = "meta"
	metaRecord["id"] = *globals.AccountID
	metaRecord["g"] = objectID
	metaRecord["tk"] = *globals.AccountToken

	appFields := make(map[string]interface{})
	appFields["Version Name"] = l.AppVersion
	appFields["App Version"] = l.AppVersion
	if l.SystemName == "iOS" || l.SystemName == "iPhone OS" {
		appFields["SDK Version"] = "30401"
		appFields["Make"] = "Apple"
	} else {
		appFields["SDK Version"] = "30403"
		appFields["Make"] = "Others"
	}
	appFields["Model"] = l.DeviceModel
	appFields["OS Version"] = l.SystemVersion
	appFields["wdt"] = 2.57
	appFields["hgt"] = 4.84
	if l.Lat != "" && l.Lon != "" {
		lat, err1 := strconv.ParseFloat(l.Lat, 64)
		lon, err2 := strconv.ParseFloat(l.Lon, 64)
		if err1 == nil && err2 == nil {
			appFields["Latitude"] = lat
			appFields["Longitude"] = lon
		}
	}

	metaRecord["af"] = appFields
	records = append(records, metaRecord)

	//data record
	dataRecord := make(map[string]interface{})
	dataRecord["pg"] = 1
	dataRecord["type"] = "data"

	data := make(map[string]interface{})
	data["id"] = ""
	data["action"] = "register"
	if l.SystemName == "iOS" || l.SystemName == "iPhone OS" {
		data["type"] = "apns"
	} else {
		data["type"] = "gcm"
	}

	dataRecord["data"] = data

	records = append(records, dataRecord)

	return records, nil
}

func (p *leanplumRecordInfo) print() {
	//log.Printf("First Result: %v", p.Results[0])
	//log.Printf("Results size: %v", len(p.Results))
}

type uploadRecordsFromLeanplum struct {
}

var s3ObjectPrefix string
var startDate string
var endDate string

var (
	s3AccessId         string
	s3SecretKey        string
	s3BucketName       string
	s3RegionName       string
	generatedFilesFile string
	lpAppID            string
	lpClientKey        string
	leanplumExportEP   = "https://www.leanplum.com/api"
)

func (u *uploadRecordsFromLeanplum) Execute() {
	log.Println("started")
	startDate = *globals.StartDate
	endDate = *globals.EndDate
	s3AccessId = *globals.AWSAccessKeyID
	s3SecretKey = *globals.AWSSecretAccessKey
	s3BucketName = *globals.S3Bucket
	s3RegionName = *globals.AWSRegion
	s3ObjectPrefix = *globals.AccountID + "-" + startDate + "-" + endDate + "/"
	if _, err := os.Stat(*globals.LeanplumOutFilesPath); os.IsNotExist(err) {
		err = os.MkdirAll(*globals.LeanplumOutFilesPath, os.ModePerm)
	}
	if !strings.HasSuffix(*globals.LeanplumOutFilesPath, "/") {
		*globals.LeanplumOutFilesPath = *globals.LeanplumOutFilesPath + "/"
	}
	generatedFilesFile = *globals.LeanplumOutFilesPath + "files-" + startDate + "-" + endDate + ".txt"
	leanplumExportEP = "https://www.leanplum.com/api"
	if *globals.LeanplumAPIEndpoint != "" {
		leanplumExportEP = *globals.LeanplumAPIEndpoint
	}
	done := make(chan interface{})
	if *globals.ImportService == "leanplumToS3" || *globals.ImportService == "leanplumToS3Throttled" {
		lpAppID = *globals.LeanplumAppID
		lpClientKey = *globals.LeanplumClientKey
		log.Printf("Fetching data from Leanplum for start date: %v and end date: %v\n", startDate, endDate)
		log.Printf("Uploading it to S3 bucket: %v with S3 object prefix: %v\n", s3BucketName, s3ObjectPrefix)
		log.Printf("Generated file names will be in: %v", generatedFilesFile)
		if *globals.ImportService == "leanplumToS3" {
			leanplumRecordsToS3Generator(done)
		} else {
			leanplumRecordsToS3GeneratorThrottled(done)
		}
		log.Printf("Fetched data from Leanplum for start date: %v and end date: %v\n", startDate, endDate)
		log.Printf("Uploaded it to S3 bucket: %v with S3 object prefix: %v\n", s3BucketName, s3ObjectPrefix)
		log.Printf("Generated file names in: %v", generatedFilesFile)
		log.Println("done")
	} else {
		if *globals.ImportService == "leanplumS3ToCT" {
			//batch size of 400 for leanplum data
			ctBatchSize = 400
			var wg sync.WaitGroup
			apiConcurrency = 9
			sdkConcurrency = 500
			apiUploadRecordStream, iosSDKRecordStream, androidSDKRecordStream := leanplumRecordsFromS3Generator(done)
			batchAndSendToCTAPI(done, processAPIRecordForUpload(done, apiUploadRecordStream), &wg)
			sendToCTSDK("https://wzrkt.com/a1?os=iOS", done, processSDKRecordForUpload(done, iosSDKRecordStream), &wg)
			sendToCTSDK("https://wzrkt.com/a1?os=android", done, processSDKRecordForUpload(done, androidSDKRecordStream), &wg)
			wg.Wait()
			log.Println("done")
			log.Printf("Data Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
		}
	}
}

func getJobID(startDate, endDate string) string {
	client := &http.Client{Timeout: time.Minute * 1}
	endpoint := leanplumExportEP + "?appId=" + lpAppID + "&clientKey=" + lpClientKey +
		"&apiVersion=1.0.6&action=exportData&startDate=" + startDate + "&endDate=" + endDate +
		"&s3BucketName=" + s3BucketName + "&s3AccessId=" + s3AccessId + "&s3AccessKey=" +
		s3SecretKey + "&s3ObjectPrefix=" + s3ObjectPrefix

	req, err := http.NewRequest("POST", endpoint, nil)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Printf("Job status code: %v\n", resp.StatusCode)
	d := json.NewDecoder(resp.Body)
	j := &jobResponse{}
	err = d.Decode(j)
	if err != nil {
		log.Fatal(err)
	}
	jobID := j.Res[0].JobID
	return jobID
}

type s3Line struct {
	line    string
	scanErr error
}

func getLinesFromS3File(scanner *bufio.Scanner) <-chan s3Line {
	s3LineChannel := make(chan s3Line)
	go func(innerScanner *bufio.Scanner) {
		defer close(s3LineChannel)
		for innerScanner.Scan() {
			s3LineChannel <- s3Line{line: innerScanner.Text(), scanErr: nil}
		}
		if err := innerScanner.Err(); err != nil {
			s3LineChannel <- s3Line{line: "", scanErr: err}
		}
	}(scanner)
	return s3LineChannel
}

func putLinesFromS3InStream(s3LineChannel <-chan s3Line,
	leanplumAPIUploadRecordStream chan<- apiUploadRecordInfo,
	leanplumSDKIOSRecordStream, leanplumSDKAndroidRecordStream chan<- sdkUploadRecordInfo,
	done chan interface{}, processedLineCount *int) (error, bool) {
	var scanErr error = nil
	i := 0
	log.Printf("Processed Count for file: %v", *processedLineCount)
	for {
		t := time.NewTimer(30 * time.Second)
		select {
		case lineFromS3, lineChannelNotClosed := <-s3LineChannel:
			scanErr = lineFromS3.scanErr
			if !lineChannelNotClosed || scanErr != nil {
				//error exists or channel has closed and scanner has stopped sending lines, done reading the entire file from S3
				if !t.Stop() {
					<-t.C
				}
				return scanErr, true
			}
			s := lineFromS3.line
			i += 1
			if i > *processedLineCount {
				s = strings.Trim(s, " \n \r")
				info := &leanplumRecordInfo{}
				jsonParseError := json.Unmarshal([]byte(s), info)
				if jsonParseError == nil {
					//json parsed correctly ignore line otherwise
					select {
					case <-done:
						return nil, false
					case leanplumAPIUploadRecordStream <- info:
					}
					if info.SystemName == "iOS" || info.SystemName == "iPhone OS" {
						select {
						case <-done:
							return nil, false
						case leanplumSDKIOSRecordStream <- info:
						}
					}
					if info.SystemName == "Android OS" {
						select {
						case <-done:
							return nil, false
						case leanplumSDKAndroidRecordStream <- info:
						}
					}
				}
				*processedLineCount++
			}
		case <-t.C:
			//timed out reading from S3 file
			scanErr = errors.New("Timed out reading from S3 file")
			return scanErr, true
		}
		if !t.Stop() {
			<-t.C
		}
	}
}

func processFile(contentKey string, leanplumAPIUploadRecordStream chan<- apiUploadRecordInfo,
	leanplumSDKIOSRecordStream, leanplumSDKAndroidRecordStream chan<- sdkUploadRecordInfo, done chan interface{}) bool {

	creds := credentials.NewStaticCredentials(s3AccessId, s3SecretKey, "")

	signer := v4.NewSigner(creds)

	processedLineCount := 0

	for {
		req, body, err := buildRequest("s3", s3RegionName, s3BucketName,
			contentKey, "")
		for err != nil {
			log.Printf("Error while building S3 request for %v: %v\n ", contentKey, err)
			log.Println("Retrying after 5 seconds")
			time.Sleep(5 * time.Second)
			req, body, err = buildRequest("s3", s3RegionName, s3BucketName,
				contentKey, "")
		}
		signer.Sign(req, body, "s3", s3RegionName, time.Now())
		client := &http.Client{Timeout: time.Minute * 240}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode < 300 {
			scanner := bufio.NewScanner(resp.Body)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 20*1024*1024)
			scanner.Split(ScanCRLF)
			s3LineChannel := getLinesFromS3File(scanner)
			scanErr, shouldContinue := putLinesFromS3InStream(s3LineChannel,
				leanplumAPIUploadRecordStream, leanplumSDKIOSRecordStream, leanplumSDKAndroidRecordStream, done,
				&processedLineCount)

			if !shouldContinue {
				return false
			}

			if scanErr != nil {
				log.Printf("Error while getting data from S3 for %v: %v : %v\n ", contentKey, scanErr, processedLineCount)
				log.Println("Retrying after 5 seconds")
				if resp != nil {
					resp.Body.Close()
				}
				time.Sleep(5 * time.Second)
				continue
			}

			resp.Body.Close()
			break
		}
		if err != nil {
			log.Println("Error while fetching events data from S3 ", err)
			log.Printf("retrying after 5 seconds for contentKey: %v", contentKey)
		} else {
			body, _ := ioutil.ReadAll(resp.Body)
			log.Println("response body: ", string(body))
			log.Printf("retrying after 5 seconds for contentKey: %v", contentKey)
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(5 * time.Second)
	}
	return true
}

//getting data from S3
func leanplumRecordsFromS3Generator(done chan interface{}) (<-chan apiUploadRecordInfo, <-chan sdkUploadRecordInfo, <-chan sdkUploadRecordInfo) {
	leanplumAPIUploadRecordStream := make(chan apiUploadRecordInfo)
	leanplumSDKIOSRecordStream := make(chan sdkUploadRecordInfo)
	leanplumSDKAndroidRecordStream := make(chan sdkUploadRecordInfo)
	go func() {
		defer func() {
			close(leanplumAPIUploadRecordStream)
			close(leanplumSDKIOSRecordStream)
			close(leanplumSDKAndroidRecordStream)
		}()

		file, err := os.Open(generatedFilesFile)
		if err != nil {
			log.Fatal("Error reading file: ", err)
			return
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 20*1024*1024)
		scanner.Split(ScanCRLF)
		for scanner.Scan() {
			contentKey := scanner.Text()
			contentKey = strings.Trim(contentKey, " \n \r")
			log.Println("Processing data from: " + contentKey)
			success := processFile(contentKey, leanplumAPIUploadRecordStream, leanplumSDKIOSRecordStream,
				leanplumSDKAndroidRecordStream, done)
			if !success {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}()
	return leanplumAPIUploadRecordStream, leanplumSDKIOSRecordStream, leanplumSDKAndroidRecordStream
}

var lpCredError = errors.New("Error: Please check your LeanPlum or S3 credentials")

func pushDataForStartEndDate(startDate, endDate string) ([]s3CopyEntryInfo, error) {
	var files []s3CopyEntryInfo
	jobID := getJobID(startDate, endDate)
	if jobID == "" {
		return nil, lpCredError
	}
	log.Printf("job id: %v", jobID)
	//http://www.leanplum.com/api?appId=appID&clientKey=clientKey&apiVersion=1.0.6&action=getExportResults&jobId=jobID
	for {
		client := &http.Client{Timeout: time.Minute * 1}
		endpoint := leanplumExportEP + "?appId=" + lpAppID + "&clientKey=" + lpClientKey + "&apiVersion=1.0.6&action=getExportResults&jobId=" + jobID
		//log.Printf("Fetching profiles data from Leanplum for page: %v", page)
		req, err := http.NewRequest("POST", endpoint, nil)
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		d := json.NewDecoder(resp.Body)
		j := &jobResponse{}
		err = d.Decode(j)
		if err != nil {
			log.Fatal(err)
		}
		state := j.Res[0].State
		if state == "FINISHED" {
			files = j.Res[0].S3CopyStatus.Success
			break
		}
		if state == "FAILED" {
			return nil, lpCredError
		}
		log.Printf("Waiting 2 minutes for files to be ready for jobID: %v , state: %v", jobID, state)
		time.Sleep(2 * time.Minute)
	}
	return files, nil
}

//saving to S3 Throttled
func leanplumRecordsToS3GeneratorThrottled(done chan interface{}) <-chan apiUploadRecordInfo {
	var wg sync.WaitGroup
	wg.Add(1)
	leanplumRecordStream := make(chan apiUploadRecordInfo)
	go func() {
		defer func() {
			close(leanplumRecordStream)
			wg.Done()
		}()
		if _, err := os.Stat(generatedFilesFile); err == nil || !os.IsNotExist(err) {
			//delete file since it exists
			err = os.Remove(generatedFilesFile)
			if err != nil {
				log.Fatal(err)
			}
		}
		file, err := os.OpenFile(generatedFilesFile,
			os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal(err)
		}

		//add five days
		sDate := startDate
		t, _ := time.Parse("20060102", sDate)
		t = t.AddDate(0, 0, 4)
		eDate := t.Format("20060102")

		for {
			eDateInt, _ := strconv.Atoi(eDate)
			endDateInt, _ := strconv.Atoi(endDate)
			if eDateInt > endDateInt {
				//reached end date
				eDate = endDate
			}

			log.Printf("Getting data for dates %v to %v", sDate, eDate)

			files, err := pushDataForStartEndDate(sDate, eDate)

			if err != nil {
				log.Fatal(err)
			}

			if files != nil {
				for i := 0; i < len(files); i++ {
					file.Write([]byte(files[i].DestFile))
					file.Write([]byte("\n"))
				}
			}

			if eDate == endDate {
				break
			}

			//add 5 days
			st, _ := time.Parse("20060102", eDate)
			st = st.AddDate(0, 0, 1)
			sDate = st.Format("20060102")
			st = st.AddDate(0, 0, 4)
			eDate = st.Format("20060102")
		}

	}()
	wg.Wait()
	return leanplumRecordStream
}

//saving to S3
func leanplumRecordsToS3Generator(done chan interface{}) <-chan apiUploadRecordInfo {
	var wg sync.WaitGroup
	wg.Add(1)
	leanplumRecordStream := make(chan apiUploadRecordInfo)
	go func() {
		defer func() {
			close(leanplumRecordStream)
			wg.Done()
		}()
		if _, err := os.Stat(generatedFilesFile); err == nil || !os.IsNotExist(err) {
			//delete file since it exists
			err = os.Remove(generatedFilesFile)
			if err != nil {
				log.Fatal(err)
			}
		}
		file, err := os.OpenFile(generatedFilesFile,
			os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal(err)
		}

		files, err := pushDataForStartEndDate(startDate, endDate)
		if err != nil {
			log.Fatal(err)
		}
		if files != nil {
			for i := 0; i < len(files); i++ {
				file.Write([]byte(files[i].DestFile))
				file.Write([]byte("\n"))
			}
		}
	}()
	wg.Wait()
	return leanplumRecordStream
}
