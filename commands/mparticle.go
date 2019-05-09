package commands

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ankit-arora/clevertap-data-upload/globals"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
)

type uploadEventsFromMParticle struct {
}

func (u *uploadEventsFromMParticle) Execute() {
	log.Println("started")
	//ct batch size of 100 for MP
	ctBatchSize = 100
	var wg sync.WaitGroup
	done := make(chan interface{})
	if globals.StartDate != nil && *globals.StartDate != "" {
		batchAndSendToCTAPI(done, processAPIRecordForUpload(done, mparticleEventRecordsGenerator(done,
			mparticleStartEndDateS3ObjectsGenerator(done))), &wg)
	} else {
		batchAndSendToCTAPI(done, processAPIRecordForUpload(done, mparticleEventRecordsGenerator(done,
			mparticleAllS3ObjectsGenerator(done))), &wg)
	}

	wg.Wait()
	log.Println("done")
	log.Println("---------------------Summary---------------------")
	log.Printf("Events Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
	if len(Summary.mpParseErrorResponses) > 0 {
		log.Println("Mparticle Events Parse Error Responses:")
		for _, parseErrorResponse := range Summary.mpParseErrorResponses {
			log.Println(parseErrorResponse)
		}
	}
}

func buildRequestWithBodyReader(serviceName, region, bucketName, objectName string, body io.Reader) (*http.Request, io.ReadSeeker, error) {
	var bodyLen int

	type lenner interface {
		Len() int
	}
	if lr, ok := body.(lenner); ok {
		bodyLen = lr.Len()
	}
	endpoint := "https://" + bucketName + "." + serviceName + "." + region + ".amazonaws.com/" + objectName
	req, err := http.NewRequest("GET", endpoint, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	if bodyLen > 0 {
		req.Header.Set("Content-Length", strconv.Itoa(bodyLen))
	}

	var seeker io.ReadSeeker
	if sr, ok := body.(io.ReadSeeker); ok {
		seeker = sr
	} else {
		seeker = aws.ReadSeekCloser(body)
	}

	return req, seeker, nil
}

func buildRequest(serviceName, region, bucketName, objectName, body string) (*http.Request, io.ReadSeeker, error) {
	reader := strings.NewReader(body)
	return buildRequestWithBodyReader(serviceName, region, bucketName, objectName, reader)
}

/*
{
"events" :
[
{
"data" : {},
"event_type" : "custom_event"
}
],
"device_info" : {},
"user_attributes" : {},
"deleted_user_attributes" : [],
"user_identities" : {},
"application_info" : {},
"schema_version": 2,
"environment" : "production",
"ip" : "127.0.0.1"
}
*/

// MparticleEventData ...
type MparticleEventData struct {
	Data      map[string]interface{} `json:"data,omitempty"`
	EventType string                 `json:"event_type,omitempty"`
}

type mparticleEventRecordInfo struct {
	Events                []MparticleEventData   `json:"events,omitempty"`
	DeviceInfo            map[string]interface{} `json:"device_info,omitempty"`
	UserAttributes        map[string]interface{} `json:"user_attributes,omitempty"`
	DeletedUserAttributes []interface{}          `json:"deleted_user_attributes,omitempty"`
	UserIdentities        map[string]interface{} `json:"user_identities,omitempty"`
	ApplicationInfo       map[string]interface{} `json:"application_info,omitempty"`
	SchemaVersion         float64                `json:"schema_version,omitempty"`
	Environment           string                 `json:"environment,omitempty"`
	IP                    string                 `json:"ip,omitempty"`
}

func (info *mparticleEventRecordInfo) convertToCTAPIFormat() ([]interface{}, error) {
	records := make([]interface{}, 0)
	for _, eventFromMParticle := range info.Events {
		eventData := eventFromMParticle.Data
		eventNameI, ok := eventData["event_name"]
		if !ok {
			log.Printf("Event name missing for record: %v . Skipping", info)
			continue
		}
		eventName := eventNameI.(string)
		if eventName == "" {
			log.Printf("Event name missing for record: %v . Skipping", info)
			continue
		}
		if globals.FilterEventsSet != nil {
			_, ok := globals.FilterEventsSet[eventName]
			if ok {
				//filter event
				log.Printf("Filtered event: %v.", eventName)
				continue
			}
		}
		record := make(map[string]interface{})
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
		record["evtName"] = eventName
		tsInterface, ok := eventData["timestamp_unixtime_ms"]
		if !ok {
			log.Printf("Time stamp is missing for record: %v . Skipping", info)
			continue
		}
		ts, err := strconv.ParseInt(tsInterface.(string), 10, 64)
		if err != nil {
			log.Printf("Time stamp is in wrong format for record: %v . Skipping", info)
			continue
		}
		record["ts"] = ts / 1000

		customAttributes := eventData["custom_attributes"].(map[string]interface{})
		userID, ok := customAttributes["user_id"]

		if ok && userID.(string) != "-1" {
			//send userId as identity
			identity := userID.(string)
			record["identity"] = identity
		} else {
			//generate objectId from advertising id
			androidAdID, ok := info.DeviceInfo["android_advertising_id"]
			if ok {
				record["objectId"] = "__g" + strings.Replace(androidAdID.(string), "-", "", -1)
			} else {
				iosAdID, ok := info.DeviceInfo["ios_advertising_id"]
				if ok {
					record["objectId"] = "-g" + strings.Replace(iosAdID.(string), "-", "", -1)
				} else {
					log.Printf("Both user_id and advertising ids are missing for record: %v . Skipping", eventFromMParticle)
					continue
				}
			}

		}

		propData := make(map[string]interface{})
		for k, v := range customAttributes {
			propData[k] = v
			if globals.Schema != nil {
				dataType, ok := globals.Schema[k]
				if ok {
					dataType = strings.ToLower(dataType)
					valueType := reflect.TypeOf(v)
					switch valueType.Kind() {
					case reflect.String:
						vTemp := v.(string)
						if dataType == "float" {
							v, err := strconv.ParseFloat(vTemp, 64)
							if err == nil {
								propData[k] = v
							}
						}
						if dataType == "integer" {
							v, err := strconv.ParseInt(vTemp, 10, 64)
							if err == nil {
								propData[k] = v
							}
						}
						if dataType == "boolean" {
							v, err := strconv.ParseBool(strings.ToLower(vTemp))
							if err == nil {
								propData[k] = v
							}
						}
						break
					case reflect.Float64:
						vTemp := v.(float64)
						if dataType == "integer" {
							propData[k] = int(vTemp)
						}
						if dataType == "string" {
							v := strconv.FormatFloat(vTemp, 'f', -1, 64)
							propData[k] = v
						}
						break
					case reflect.Bool:
						vTemp := v.(bool)
						if dataType == "string" {
							v := strconv.FormatBool(vTemp)
							propData[k] = v
						}
						break
					default:
					}
				}
			}
		}
		record["evtData"] = propData
		record["type"] = "event"
		records = append(records, record)
	}

	return records, nil
}

func (info *mparticleEventRecordInfo) print() {
	//fmt.Printf("\nresponse: %v", e.response)
}

func getCommonPrefixes(svc *s3.S3) ([]string, error) {
	marker := ""
	commonPrefixes := make([]string, 0)
	for {
		input := &s3.ListObjectsInput{
			Bucket:    aws.String(*globals.S3Bucket),
			Marker:    aws.String(marker),
			Prefix:    aws.String(""),
			Delimiter: aws.String("/"),
		}
		result, err := svc.ListObjects(input)

		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					log.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
				default:
					log.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				log.Println(err.Error())
			}
			return nil, err
		}

		commonPs := result.CommonPrefixes

		for _, cp := range commonPs {
			commonPrefixes = append(commonPrefixes, *cp.Prefix)
		}

		if len(result.Contents) == 0 {
			break
		}

		marker = *result.Contents[len(result.Contents)-1].Key

	}

	return commonPrefixes, nil
}

//android/2018-10-02

func mparticleStartEndDateS3ObjectsGenerator(done chan interface{}) <-chan []*s3.Object {
	mparticleObjectsStream := make(chan []*s3.Object)
	go func() {
		defer close(mparticleObjectsStream)
		creds := credentials.NewStaticCredentials(*globals.AWSAccessKeyID,
			*globals.AWSSecretAccessKey, "")
		sess, _ := session.NewSession(&aws.Config{
			Region:      aws.String(*globals.AWSRegion),
			Credentials: creds,
		},
		)

		svc := s3.New(sess)
		prefixes, err := getCommonPrefixes(svc)

		if err != nil {
			select {
			case <-done:
				return
			default:
				done <- struct{}{}
				return
			}
		}

		eventsDate := *globals.StartDate
		endDate := *globals.EndDate

		if endDate == "" {
			endDate = time.Now().Local().Format("2006-01-02")
		}

		log.Printf("Fetching events with start date: %v and end date: %v ", eventsDate, endDate)

		for _, prefix := range prefixes {
			//for each prefix
			eventsDate = *globals.StartDate
			for {
				//for each date between start and end date
				marker := ""
				for {
					input := &s3.ListObjectsInput{
						Bucket: aws.String(*globals.S3Bucket),
						Marker: aws.String(marker),
						Prefix: aws.String(prefix + eventsDate),
					}
					result, err := svc.ListObjects(input)

					if err != nil {
						if aerr, ok := err.(awserr.Error); ok {
							switch aerr.Code() {
							case s3.ErrCodeNoSuchBucket:
								log.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
							default:
								log.Println(aerr.Error())
							}
						} else {
							// Print the error, cast err to awserr.Error to get the Code and
							// Message from an error.
							log.Println(err.Error())
						}

						select {
						case <-done:
							return
						default:
							done <- struct{}{}
							return
						}
					}

					select {
					case <-done:
						return
					case mparticleObjectsStream <- result.Contents:
					}

					//for _, obj := range result.Contents {
					//	fmt.Println(*obj.Key)
					//}

					if len(result.Contents) == 0 {
						break
					}

					marker = *result.Contents[len(result.Contents)-1].Key
				}

				if eventsDate == endDate {
					//reached end date
					break
				}

				t, _ := time.Parse("2006-01-02", eventsDate)
				t = t.AddDate(0, 0, 1)
				eventsDate = t.Format("2006-01-02")
			}
		}
	}()

	return mparticleObjectsStream
}

func mparticleAllS3ObjectsGenerator(done chan interface{}) <-chan []*s3.Object {
	mparticleObjectsStream := make(chan []*s3.Object)
	go func() {
		defer close(mparticleObjectsStream)
		creds := credentials.NewStaticCredentials(*globals.AWSAccessKeyID,
			*globals.AWSSecretAccessKey, "")
		sess, _ := session.NewSession(&aws.Config{
			Region:      aws.String(*globals.AWSRegion),
			Credentials: creds,
		},
		)

		svc := s3.New(sess)
		marker := ""
		for {
			//input := &s3.ListObjectsInput{
			//	Bucket: aws.String(*globals.S3Bucket),
			//	Marker: aws.String(marker),
			//	Prefix: aws.String("*/2018-10-02/"),
			//}
			input := &s3.ListObjectsInput{
				Bucket: aws.String(*globals.S3Bucket),
				Marker: aws.String(marker),
				Prefix: aws.String(""),
			}
			result, err := svc.ListObjects(input)

			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case s3.ErrCodeNoSuchBucket:
						log.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
					default:
						log.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					log.Println(err.Error())
				}

				select {
				case <-done:
					return
				default:
					done <- struct{}{}
					return
				}

			}

			select {
			case <-done:
				return
			case mparticleObjectsStream <- result.Contents:
			}

			if len(result.Contents) == 0 {
				break
			}

			marker = *result.Contents[len(result.Contents)-1].Key

			//log.Println("marker:", marker)

		}
	}()

	return mparticleObjectsStream
}

func mparticleEventRecordsGenerator(done chan interface{}, inputBucketStream <-chan []*s3.Object) <-chan apiUploadRecordInfo {
	mparticleRecordStream := make(chan apiUploadRecordInfo)
	go func() {
		defer close(mparticleRecordStream)
		creds := credentials.NewStaticCredentials(*globals.AWSAccessKeyID,
			*globals.AWSSecretAccessKey, "")

		signer := v4.NewSigner(creds)

		for objects := range inputBucketStream {
			for _, content := range objects {
				log.Printf("Processing file %v\n", *content.Key)
				for {
					req, body, err := buildRequest("s3", *globals.AWSRegion, *globals.S3Bucket,
						*content.Key, "")
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
					signer.Sign(req, body, "s3", *globals.AWSRegion, time.Now())
					client := &http.Client{}
					resp, err := client.Do(req)
					if err == nil && resp.StatusCode < 300 {
						scanner := bufio.NewScanner(resp.Body)
						buf := make([]byte, 0, 64*1024)
						scanner.Buffer(buf, 20*1024*1024)
						scanner.Split(ScanCRLF)
						for scanner.Scan() {
							s := scanner.Text()
							s = strings.Trim(s, " \n \r")
							info := &mparticleEventRecordInfo{}
							err = json.Unmarshal([]byte(s), info)
							//fmt.Printf("\nline: %v\n", s)
							//customAttributes := info.Events[0].Data["custom_attributes"].(map[string]interface{})
							//fmt.Println("user id: ", customAttributes["user_id"])
							select {
							case <-done:
								return
							case mparticleRecordStream <- info:
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

						break

					}
					if err != nil {
						log.Println("Error while fetching events data from Mparticle ", err)
						log.Printf("retrying after 20 seconds for file: %v", *content.Key)
					} else {
						body, _ := ioutil.ReadAll(resp.Body)
						log.Println("response body: ", string(body))
						log.Printf("retrying after 20 seconds for file: %v", *content.Key)
					}
					if resp != nil {
						resp.Body.Close()
					}
					time.Sleep(20 * time.Second)
				}
			}
		}
	}()
	return mparticleRecordStream
}
