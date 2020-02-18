package commands

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

const (
	amplitudeExportEP = "https://amplitude.com/api/2/export"
)

type uploadDataFromAmplitude struct {
}

func (u *uploadDataFromAmplitude) Execute() {

	done := make(chan interface{})
	//batch size of 400 for amplitude data
	ctBatchSize = 400
	//var wg sync.WaitGroup
	//apiConcurrency = 3
	//amplitudeEventsUploadRecordStream, amplitudeProfilesUploadRecordStream := amplitudeDataGenerator(done)
	//batchAndSendToCTAPI(done, processAPIRecordForUpload(done, amplitudeEventsUploadRecordStream), &wg)
	//batchAndSendToCTAPI(done, processAPIRecordForUpload(done, amplitudeProfilesUploadRecordStream), &wg)
	//wg.Wait()
	amplitudeDataGenerator(done)
	log.Println("done")
	log.Printf("Data Processed: %v , Unprocessed: %v", Summary.ctProcessed, Summary.ctUnprocessed)
}

type amplitudeEventRecordInfo struct {
	Event      string                 `json:"event,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

type amplitudeProfileRecordInfo struct {
	Event      string                 `json:"event,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
}

func downloadDataZipFile() error {

	file, err := os.OpenFile(*globals.AmplitudeZipFilePath,
		os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Fatal("unable to close file")
		}
	}()
	start := *globals.AmplitudeStart
	end := *globals.AmplitudeEnd

	log.Printf("Fetching data with start date: %v and end date: %v ", start, end)
	encodedSecret := base64.StdEncoding.EncodeToString([]byte(*globals.AmplitudeAPIKey + ":" + *globals.AmplitudeSecretKey))

	endpoint := fmt.Sprintf(amplitudeExportEP+"?start=%v&end=%v", start, end)

	client := &http.Client{}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Basic "+encodedSecret)
	resp, err := client.Do(req)
	if err == nil && resp.StatusCode <= 500 {
		if resp.StatusCode >= 400 {
			body, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return errors.New(fmt.Sprintf("Error downloading zip file from Amplitude: %v : status: %v", body, resp.Status))
		}
		bytesBuffer := make([]byte, 1000000)
		for {
			nBytesRead, err := resp.Body.Read(bytesBuffer)
			_, _ = file.Write(bytesBuffer[:nBytesRead])
			if err != nil {
				if err == io.EOF {
					fmt.Printf("Downloaded zip file: %v\n", *globals.AmplitudeZipFilePath)
					return nil
				}
				return err
			}
		}
	}
	if resp != nil {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return errors.New(fmt.Sprintf("Error downloading zip file from Amplitude: %v : status: %v", body, resp.Status))
	}
	return errors.New("error downloading zip file from Amplitude")
}

func unzipAndCopy(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {

		fpath := filepath.Join(dest, f.Name)

		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fpath)
		}

		filenames = append(filenames, fpath)

		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return filenames, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, err
		}
	}
	return filenames, nil
}

func amplitudeDataGenerator(done chan interface{}) (<-chan apiUploadRecordInfo, <-chan apiUploadRecordInfo) {
	amplitudeEventsAPIUploadRecordStream := make(chan apiUploadRecordInfo)
	amplitudeProfilesAPIRecordStream := make(chan apiUploadRecordInfo)
	func() {
		defer func() {
			close(amplitudeEventsAPIUploadRecordStream)
			close(amplitudeProfilesAPIRecordStream)
		}()
		err := downloadDataZipFile()
		if err != nil {
			log.Fatal(err)
		}
		fileNames, err := unzipAndCopy(*globals.AmplitudeZipFilePath, "amplitude-out-test")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(fileNames)
		for _, fileName := range fileNames {
			f, err := os.Open(fileName)
			if err != nil {
				log.Fatal(err)
			}
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
			gzr, err := gzip.NewReader(f)
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(gzr)
			scanner.Split(ScanCRLF)
			for scanner.Scan() {
				s := scanner.Text()
				s = strings.Trim(s, " \n \r")
				fmt.Println(s)
				//info := &mixpanelEventRecordInfo{}
				//err = json.Unmarshal([]byte(s), info)
				//if err != nil {
				//	log.Printf("Error parsing event record %v. Skipping", s)
				//	Summary.mpParseErrorResponses = append(Summary.mpParseErrorResponses, s)
				//} else {
				//	if ts, ok := info.Properties["time"]; ok {
				//		if *globals.StartTs > 0 && ts.(float64) < *globals.StartTs {
				//			//log.Printf("start ts: %v , ts: %v", *globals.StartTs, ts.(float64))
				//			continue
				//		}
				//	}
				//	select {
				//	case <-done:
				//		file.Close()
				//		return
				//	case mixpanelRecordStream <- info:
				//	}
				//}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
				select {
				case <-done:
					f.Close()
					return
				default:
					done <- struct{}{}
					f.Close()
					return
				}
			}

			f.Close()
		}
		//client := &http.Client{Timeout: time.Minute * 240}
		//
		//req, err := http.NewRequest("GET", endpoint, nil)
		//if err != nil {
		//	log.Fatal(err)
		//	select {
		//	case <-done:
		//		return
		//	default:
		//		done <- struct{}{}
		//		return
		//	}
		//}
		//
		//resp, err := client.Do(req)
		//if err == nil && resp.StatusCode < 300 {
		//	scanner := bufio.NewScanner(resp.Body)
		//	scanner.Split(ScanCRLF)
		//	for scanner.Scan() {
		//		s := scanner.Text()
		//		s = strings.Trim(s, " \n \r")
		//		fmt.Println(s)
		//		//info := &mixpanelEventRecordInfo{}
		//		//err = json.Unmarshal([]byte(s), info)
		//		//if err != nil {
		//		//	log.Printf("Error parsing event record %v. Skipping", s)
		//		//	Summary.mpParseErrorResponses = append(Summary.mpParseErrorResponses, s)
		//		//} else {
		//		//	if ts, ok := info.Properties["time"]; ok {
		//		//		if *globals.StartTs > 0 && ts.(float64) < *globals.StartTs {
		//		//			//log.Printf("start ts: %v , ts: %v", *globals.StartTs, ts.(float64))
		//		//			continue
		//		//		}
		//		//	}
		//		//	select {
		//		//	case <-done:
		//		//		return
		//		//	case mixpanelRecordStream <- info:
		//		//	}
		//		//}
		//	}
		//	if err := scanner.Err(); err != nil {
		//		log.Fatal(err)
		//		select {
		//		case <-done:
		//			return
		//		default:
		//			done <- struct{}{}
		//			return
		//		}
		//	}
		//	if resp != nil {
		//		resp.Body.Close()
		//	}
		//	return
		//}
		//if err != nil {
		//	log.Println("Error while fetching events data from Mixpanel: ", err)
		//} else {
		//	body, _ := ioutil.ReadAll(resp.Body)
		//	log.Println("response body: ", string(body))
		//}
		//if resp != nil {
		//	resp.Body.Close()
		//}
	}()
	return amplitudeEventsAPIUploadRecordStream, amplitudeProfilesAPIRecordStream
}
