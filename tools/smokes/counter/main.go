package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	primeCount uint64
	msgCount   uint64
)

func main() {
	http.Handle("/get", http.HandlerFunc(getCountHandler))
	http.Handle("/get-prime", http.HandlerFunc(getPrimeCountHandler))

	http.Handle("/set", http.HandlerFunc(setCountHandler))

	addr := fmt.Sprintf(":%s", os.Getenv("PORT"))
	log.Println(http.ListenAndServe(addr, nil))
}

func getCountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("GET /get")

	w.Write([]byte(fmt.Sprint(atomic.LoadUint64(&msgCount))))
}

func getPrimeCountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("GET /get-prime")

	w.Write([]byte(fmt.Sprint(atomic.LoadUint64(&primeCount))))
}

func setCountHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	log.Println("POST /set")

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	parts := strings.Split(string(body), ":")
	if len(parts) != 2 {
		log.Printf("invalid set payload: length not 2: %d", len(parts))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	newPrimeCount, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		log.Printf("invalid set payload: prime count invalid: %s", parts[0])
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	newMsgCount, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		log.Printf("invalid set payload: msg count invalid: %s", parts[1])
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	atomic.SwapUint64(&primeCount, newPrimeCount)
	atomic.SwapUint64(&msgCount, newMsgCount)
}
