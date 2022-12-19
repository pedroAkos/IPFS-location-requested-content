package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"find_providers/pkg/broker"
	"find_providers/pkg/db"
	"find_providers/pkg/model"
	"find_providers/pkg/service"
	"fmt"
	"github.com/spf13/pflag"
	"sync"

	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

// postgres params
var pconf = db.PostgresConf{
	Host:     "db",
	Port:     5432,
	User:     "postgres",
	Password: "",
	DBname:   "ipfs_content_location",
}

// influxdb params
var iconf = db.InfluxDBConf{
	Org:    "my-org",
	Bucket: "my-bucket",
	DBUrl:  "http://db:8086",
	Token:  "my-super-secret-auth-token",
}

// kafka params
const (
	bootstrapServers  = "kafka:9092"
	groupId           = "ipfs-gateway-logs"
	maxPollIntervalMs = "3600000"
)

// rabbitmq params
const (
	rabbitmqHost = "amqp://guest:guest@broker:5672/"
)

const brokerToUse = "rabbitmq"

const parserUrl = "http://parser:9000"
const providersUrl = "http://find_providers:10000"

const dbToUse = "postgres"

var dbAPI *db.DB

var providersFoundLock *sync.Mutex
var providersFound map[string]time.Time

var requestsLock *sync.Mutex
var requests = 0

var count = 0

func incRequests() {
	requestsLock.Lock()
	defer requestsLock.Unlock()
	requests++
}

func decRequests() {
	requestsLock.Lock()
	defer requestsLock.Unlock()
	requests--
	count++
}

func main() {
	//var err error
	c := pflag.IntP("concurrency", "c", 100, "how many requests to process in parallel")
	b := pflag.IntP("batch", "b", 100, "how many processed requests to wait after")
	dontFindProviders := pflag.BoolP("dont-find-providers", "d", false, "Don't find providers")
	pflag.Parse()
	concurrency := *c
	var batch = *b
	var waitFor = 50

	// init db
	dbAPI = db.PrepareDB(dbToUse, pconf)

	// init controller state
	requestsLock = new(sync.Mutex)
	providersFoundLock = new(sync.Mutex)
	providersFound = make(map[string]time.Time)
	cleanup := time.NewTicker(12 * time.Hour)
	reqsCh := make(chan struct{}, concurrency)
	provsCh := make(chan struct {
		timeOfReq time.Time
		timeNow   time.Time
		reqId     string
		ans       model.JsonAnswer
		err       error
	})

	// init broker
	logCh := broker.PrepareBroker(brokerToUse, rabbitmqHost, groupId)

	// init fetch providers goroutine
	go fetchProviders(provsCh, parserUrl)

	requests := 0
	log.Infoln("Ready to go! concurrency:", concurrency, "batch:", batch)
	for {
		// forever

		// wait if there are too many ongoing requests
		if batch > 0 && count >= batch {
			requestsLock.Lock()
			if requests > waitFor {
				log.Debug("----------------- waiting ----------------")
				<-time.After(10 * time.Second)
			}
			count = 0
			requestsLock.Unlock()

		}

		log.Debug("-------------------- Requests:", requests)
		select {
		// retrieve a log entry from the broker
		case entry := <-logCh:
			reqsCh <- struct{}{}
			//parse the entry
			e, err := parseEntry(parserUrl, entry)
			reqId := genReqId(e)
			if err != nil {
				log.Warning("Error on parsing log entry:", entry, err)
				<-reqsCh
			} else {
				requests++
				// write entry to db
				go dbAPI.WriteEntryToDB(e, reqId)
				if *dontFindProviders {
					<-reqsCh
				} else {
					// providers have not been found yet
					if !foundProviders(e.Cid) {
						incRequests()
						// go and ask to find the providers for the cid
						go func(url string, cid string, t time.Time) {
							ans := struct {
								timeOfReq time.Time
								timeNow   time.Time
								reqId     string
								ans       model.JsonAnswer
								err       error
							}{timeOfReq: t, timeNow: time.Now(), reqId: reqId}
							a, e := findAllProvider(url, cid)
							ans.ans = a
							ans.err = e
							<-reqsCh
							provsCh <- ans
						}(providersUrl, e.Cid, e.Time)
					} else {
						decRequests()
						<-reqsCh
					}
				}
			}
		case <-cleanup.C:
			// cleanup providersFound map
			cleanupFoundProviders()
		}
	}

}

// cleanupFoundProviders removes from the providersFound map the entries that are older than 12 hours
func cleanupFoundProviders() {
	providersFoundLock.Lock()
	defer providersFoundLock.Unlock()
	for p, t := range providersFound {
		if time.Now().After(t.Add(24 * time.Hour)) {
			delete(providersFound, p)
		}
	}
}

// foundProviders checks if the providers for the given cid have been found
func foundProviders(cid string) bool {
	providersFoundLock.Lock()
	defer providersFoundLock.Unlock()
	t, ok := providersFound[cid]
	if ok && time.Now().After(t.Add(24*time.Hour)) {
		return false
	}
	return ok
}

// genReqId generates a unique id for the request
func genReqId(e model.EntryStruct) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v%v%v%v%v%v%v", e.Time, e.Ip, e.Cid, e.BodyBytes, e.RequestTime, e.RequestLength, e.HttpUserAgent)))
	return string(h.Sum(nil))
}

// fetchProviders fetches the providers from the providersUrl and writes them to the db
func fetchProviders(provsCh chan struct {
	timeOfReq time.Time
	timeNow   time.Time
	reqId     string
	ans       model.JsonAnswer
	err       error
}, parserUrl string) {

	var err error
	for {
		providers := <-provsCh
		//requests--
		decRequests()
		if providers.err != nil {
			log.Warning("Error on fetching providers:", providers.err)
		} else {
			log.Debug("Received providers for cid:", providers.ans.Cid, "dur:", providers.ans.Dur)
			if len(providers.ans.Providers) > 0 && foundProvider(providers.ans.Cid) {
				go func(url string, timeOfReq time.Time, timeNow time.Time, ans model.JsonAnswer, reqId string) {
					ans.Providers, err = parseProviders(url, ans.Providers)
					if err != nil {
						log.Warning("Error on parsing providers:", err)
					} else {
						dbAPI.WriteProvidersToDB(providers.timeOfReq, providers.timeNow, providers.ans)
					}
				}(parserUrl, providers.timeOfReq, providers.timeNow, providers.ans, providers.reqId)
			}
		}
	}

}

// foundProvider writes or updates the providersFound map for the given cid
func foundProvider(cid string) bool {
	providersFoundLock.Lock()
	defer providersFoundLock.Unlock()
	t, ok := providersFound[cid]
	if !ok || time.Now().After(t.Add(24*time.Hour)) {
		providersFound[cid] = time.Now()
		return true
	}
	return false
}

// parseEntry parses the log entry with the parserUrl
func parseEntry(url string, entry string) (model.EntryStruct, error) {
	resp := service.SendRequest("POST", fmt.Sprintf("%v/parse", url), "text/plain; charset=utf-8", bytes.NewBuffer([]byte(entry)))
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var e model.EntryStruct
	if resp.Status != "200 OK" {
		var errMsg struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &errMsg)
		return e, errors.New(fmt.Sprintf("%v: %v", resp.Status, errMsg.Error))
	}
	err := json.Unmarshal(bodyBytes, &e)
	if err != nil {
		panic(err)
	}

	return e, nil
}

// findAllProvider asks the providersUrl to find the providers for the given cid
func findAllProvider(url string, cid string) (model.JsonAnswer, error) {
	resp := service.SendRequest("GET", fmt.Sprintf("%v/findAllProviders/%v", url, cid), "", nil)

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans model.JsonAnswer
	if resp.Status != "200 OK" {
		var errMsg struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &errMsg)
		return ans, errors.New(fmt.Sprintf("%v: %v", resp.Status, errMsg.Error))
	}
	err := json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans, nil
}

// findProvider asks the providersUrl to find the provider for the given cid
func findProvider(url string, cid string) (model.JsonAnswer, error) {
	resp := service.SendRequest("GET", fmt.Sprintf("%v/findProviders/%v", url, cid), "", nil)

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans model.JsonAnswer
	if resp.Status != "200 OK" {
		var errMsg struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &errMsg)
		return ans, errors.New(fmt.Sprintf("%v: %v", resp.Status, errMsg.Error))
	}
	err := json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans, nil
}

// parseProviders parses the providers with the parserUrl
func parseProviders(url string, providers []model.Provider) ([]model.Provider, error) {
	providersJson, _ := json.Marshal(providers)

	resp := service.SendRequest("POST", fmt.Sprintf("%v/locate_providers", url), "application/json; charset=utf-8", bytes.NewBuffer(providersJson))

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.Status != "200 OK" {
		return providers, errors.New(resp.Status)
	}

	err := json.Unmarshal(bodyBytes, &providers)
	if err != nil {
		panic(err)
	}

	return providers, nil
}
