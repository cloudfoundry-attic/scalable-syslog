package app_test

import (
	"io/ioutil"
	"log"
)

//go:generate go get github.com/loggregator/go-bindata/...
//go:generate ../../../../../scripts/generate-certs
//go:generate go-bindata -o bindata_test.go -nocompress -pkg app_test -prefix certs/ certs/
//go:generate rm -rf certs

func Cert(filename string) string {
	contents := MustAsset(filename)
	tmpfile, err := ioutil.TempFile("", "")

	if err != nil {
		log.Fatal(err)
	}

	if _, err := tmpfile.Write(contents); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return tmpfile.Name()
}
