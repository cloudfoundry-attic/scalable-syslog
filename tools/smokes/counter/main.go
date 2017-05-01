package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
)

var count uint64

func main() {
	http.Handle("/get", http.HandlerFunc(getCountHandler))
	http.Handle("/set", http.HandlerFunc(setCountHandler))

	addr := fmt.Sprintf(":%s", os.Getenv("PORT"))
	log.Println(http.ListenAndServe(addr, nil))
}

func getCountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("GET /get")

	w.Write([]byte(fmt.Sprint(atomic.LoadUint64(&count))))
}

func setCountHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	log.Println("POST /set")

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	newCount, err := strconv.ParseUint(string(body), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	atomic.SwapUint64(&count, newCount)
}
