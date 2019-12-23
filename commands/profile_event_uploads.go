package commands

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"sync"

	"time"

	"strconv"

	"log"

	"encoding/csv"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

const (
	uploadEndpoint = "api.clevertap.com/1/upload"
)

var apiConcurrency = 3
var sdkConcurrency = 400

var ctBatchSize = 1000

type uploadEventsProfilesFromCSVCommand struct {
}

func (u *uploadEventsProfilesFromCSVCommand) Execute() {
	log.Println("started")

	done := make(chan interface{})

	var wg sync.WaitGroup

	if *globals.CSVFilePath != "" {
		batchAndSendToCTAPI(done, processCSVLineForUpload(done, csvLineGenerator(done)), &wg)
	}

	if *globals.JSONFilePath != "" {
		batchAndSendToCTAPI(done, jsonLineGenerator(done), &wg)
	}

	wg.Wait()

	log.Println("done")

	log.Println("---------------------Summary---------------------")
	if *globals.Type == "profile" {
		log.Printf("Profiles Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
	} else {
		log.Printf("Events Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
	}
}

func jsonLineGenerator(done chan interface{}) <-chan interface{} {
	recordStream := make(chan interface{})
	go func() {
		defer close(recordStream)
		//read json file
		file, err := os.Open(*globals.JSONFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Split(ScanCRLF)
		for scanner.Scan() {
			s := scanner.Text()
			s = strings.Trim(s, " \n \r")
			var jsonData interface{}
			err = json.NewDecoder(strings.NewReader(s)).Decode(&jsonData)
			if err != nil {
				log.Printf("Error in processing json record: %s : %s\n", s, err)
			} else {
				select {
				case <-done:
					return
				case recordStream <- jsonData:
				}
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}()
	return recordStream
}

//identity, objectID, FBID or GPID

var headerKeys []string
var keysLen int
var tsExists = false

func isIdentity(val string) bool {
	if val == "identity" || val == "objectId" || val == "FBID" || val == "GPID" {
		return true
	}
	return false
}

func substr(input string, start int, length int) string {
	asRunes := []rune(input)
	if start >= len(asRunes) {
		return ""
	}
	if start+length > len(asRunes) {
		length = len(asRunes) - start
	}
	return string(asRunes[start : start+length])
}

func cleanKeys(keys []string) []string {
	cleanKeys := make([]string, len(keys))
	i := 0
	for _, val := range keys {
		if strings.HasPrefix(val, "\ufeff") {
			val = substr(val, 1, len(val))
		}
		cleanKeys[i] = val
		i++
	}
	return cleanKeys
}

func processHeader(keys []string) bool {
	keys = cleanKeys(keys)
	identityExists := false

	for _, val := range keys {
		if isIdentity(val) {
			identityExists = true
		}
		if val == "ts" {
			tsExists = true
		}
	}
	if !identityExists {
		log.Println("identity, objectID, FBID or GPID should be present")
		return false
	}

	if !tsExists {
		log.Println("ts is missing. It will default to the current timestamp")
	}
	keysLen = len(keys)
	headerKeys = keys
	return true
}

func processCSVUploadLine(vals []string, line string) (interface{}, bool) {
	rowLen := len(vals)
	if rowLen != keysLen {
		log.Println("Mismatch in header and row data length")
		return nil, false
	}
	record := make(map[string]interface{})
	if !tsExists {
		record["ts"] = time.Now().Unix()
	}
	record["type"] = *globals.Type
	if *globals.Type == "event" {
		record["evtName"] = *globals.EvtName
	}
	propertyData := make(map[string]interface{})

	for index, ep := range vals {
		key := headerKeys[index]
		if isIdentity(key) {
			if ep == "" {
				log.Println("Identity field is missing.")
				return nil, false
			}
			record[key] = ep
			continue
		}

		if key == "evtName" && *globals.Type == "event" {
			if ep != *globals.EvtName {
				log.Println("Event name in record is different from command line option.")
				return nil, false
			}
			continue
		}

		if key == "ts" {
			epTs := time.Now().Unix()
			if ep == "" {
				log.Println("Timestamp is missing. It will default to the current timestamp for: ")
				log.Println(line)
				record["ts"] = epTs
				continue
			}

			tsVal := ep

			if globals.Schema != nil {
				dataType, ok := globals.Schema[key]
				if ok {
					dataTypeLower := strings.ToLower(dataType)
					if strings.HasPrefix(dataTypeLower, "date") {
						split := strings.Split(dataType, "$")
						t, err := time.Parse(split[1], tsVal+" "+split[2])
						if err != nil {
							log.Println("Timestamp is in wrong format. Should be in " + dataType)
							return nil, false
						}
						epTs = t.Unix()
					}
				}
			} else {
				epI, err := strconv.Atoi(tsVal)

				if err != nil {
					log.Println("Timestamp is in wrong format. Should be an epoch in seconds")
					return nil, false
				}

				epTs = int64(epI)
			}

			record["ts"] = epTs
			continue
		}

		if *globals.Type == "profile" && ep == "" {
			continue
		}

		if globals.Schema != nil {
			dataType, ok := globals.Schema[key]
			if ok {
				dataTypeLower := strings.ToLower(dataType)
				if strings.HasPrefix(dataTypeLower, "date") {
					split := strings.Split(dataType, "$")
					t, err := time.Parse(split[1], ep+" "+split[2])
					if err == nil {
						epoch := t.Unix()
						propertyData[key] = "$D_" + strconv.FormatInt(epoch, 10)
					}
				}
				dataType = strings.ToLower(dataType)
				if dataType == "float" {
					v, err := strconv.ParseFloat(ep, 64)
					if err == nil {
						propertyData[key] = v
					}
				}
				if dataType == "integer" {
					v, err := strconv.ParseInt(ep, 10, 64)
					if err == nil {
						propertyData[key] = v
					}
				}
				if dataType == "boolean" {
					v, err := strconv.ParseBool(strings.ToLower(ep))
					if err == nil {
						propertyData[key] = v
					}
				}
				if *globals.Type == "profile" {

					if dataType == "integer[]" {
						v, err := strconv.ParseInt(ep, 10, 64)
						if err == nil {
							addArray := make(map[string][]int64)
							addArray["$add"] = append(addArray["$add"], v)

							propertyData[key] = addArray

						}
					}

					if dataType == "string[]" {

						addArray := make(map[string][]string)
						addArray["$add"] = append(addArray["$add"], ep)

						propertyData[key] = addArray

					}

					if dataType == "boolean[]" {
						v, err := strconv.ParseBool(strings.ToLower(ep))
						if err == nil {
							addArray := make(map[string][]bool)
							addArray["$add"] = append(addArray["$add"], v)

							propertyData[key] = addArray

						}
					}
					if dataType == "float[]" {
						v, err := strconv.ParseFloat(ep, 64)
						if err == nil {
							addArray := make(map[string][]float64)
							addArray["$add"] = append(addArray["$add"], v)

							propertyData[key] = addArray
						}
					}
				}
			}
		}
		_, ok := propertyData[key]
		if !ok {
			propertyData[key] = ep
		}
	}

	if *globals.Type == "event" {
		record["evtData"] = propertyData
	}
	if *globals.Type == "profile" {
		record["profileData"] = propertyData
	}

	return record, true
}

func processCSVLineForUpload(done chan interface{}, rowStream <-chan csvLineInfo) <-chan interface{} {
	recordStream := make(chan interface{})
	go func() {
		defer close(recordStream)
		for lineInfo := range rowStream {
			i := lineInfo.LineNum
			l := lineInfo.Line
			//sLine := strings.Split(l, ",")
			r := csv.NewReader(strings.NewReader(l))
			sLineArr, err := r.ReadAll()
			if err != nil || len(sLineArr) != 1 {
				if i == 0 {
					log.Println("Error in processing header")
					select {
					case <-done:
						return
					default:
						done <- struct{}{}
						log.Println("...Exiting...")
						return
					}
				}
				if l != "" {
					log.Printf("Error in processing record")
					log.Printf("Skipping line number: %v : %v", i+1, l)
				}
				continue
			}
			sLine := sLineArr[0]
			if i == 0 {
				//header: line just process to get keys
				if !processHeader(sLine) {
					select {
					case <-done:
						return
					default:
						done <- struct{}{}
						log.Println("...Exiting...")
						return
					}
				}
			} else {
				record, shouldAdd := processCSVUploadLine(sLine, l)
				if shouldAdd {
					select {
					case <-done:
						return
					case recordStream <- record:
					}
				} else {
					log.Println("Skipping line number: ", i+1, " : ", l)
				}
			}
		}
	}()
	return recordStream
}
