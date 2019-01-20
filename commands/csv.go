package commands

import (
	"bytes"
	"log"
	"os"

	"bufio"

	"strings"

	"github.com/ankit-arora/clevertap-data-upload/globals"
)

type csvLineInfo struct {
	LineNum int
	Line    string
}

// ScanCRLF ...
func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	i := bytes.IndexByte(data, '\n')
	j := bytes.IndexByte(data, '\r')
	if i >= 0 || j >= 0 {
		// We have a full newline-terminated line.
		if i >= 0 {
			return i + 1, data[0:i], nil
		}
		//fmt.Println("data:-" + string(data[0:j]))
		return j + 1, data[0:j], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

func csvLineGenerator(done chan interface{}) <-chan csvLineInfo {
	rowStream := make(chan csvLineInfo)
	go func() {
		defer close(rowStream)
		//read csv file
		file, err := os.Open(*globals.CSVFilePath)
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
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Split(ScanCRLF)
		i := 0
		for scanner.Scan() {
			s := scanner.Text()
			s = strings.Trim(s, " \n \r")
			info := csvLineInfo{i, s}
			select {
			case <-done:
				return
			case rowStream <- info:
			}
			i++
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
	}()
	return rowStream
}
