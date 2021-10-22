package s3sync

import (
	"fmt"
	"mime"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func init() {
	mime.AddExtensionType(".map", "application/json")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getMetadataString(contentType string, contentEncoding *string) (metadataString string) {
	metadataString = fmt.Sprintf("Content-Type: %s", contentType)
	if contentEncoding != nil {
		metadataString += "Content-Encoding: " + *contentEncoding
	}
	return metadataString
}

func guessFileMetadata(filename string) (contentType string, contentEncoding *string) {

	split := strings.Split(filename, ".")[1:]

	// Naive detection of gzip compressed files
	if split[len(split)-1] == "gz" {
		contentEncoding = aws.String("gzip")
		if len(split) != 0 {
			split = split[:len(split)-1]
		}
	}

	for i := range split {
		toGuess := ""
		for i2 := i; i2 < len(split); i2++ {
			toGuess += "." + split[i2]
		}
		guess := mime.TypeByExtension(toGuess)
		if guess != "" {
			contentType = guess
			break
		}
	}
	if contentType == "" {
		if *contentEncoding == "gzip" {
			// It's a gzip file
			contentType = "application/gzip"
			contentEncoding = nil
		} else {
			// no idea what this file is, fallback to octet stream
			contentType = "application/octet-stream"
		}
	}

	return contentType, contentEncoding
}
