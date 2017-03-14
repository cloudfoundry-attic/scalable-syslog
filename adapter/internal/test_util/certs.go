package test_util

import (
	"io/ioutil"
	"log"
)

//go:generate ../../../scripts/generate-certs no-ca
//go:generate go-bindata -o bindata.go -nocompress -pkg test_util -prefix scalable-syslog-certs/ scalable-syslog-certs/

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
