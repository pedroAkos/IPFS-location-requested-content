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
	bootstrap_servers    = "kafka:9092"
	group_id             = "ipfs-gateway-logs"
	max_poll_interval_ms = "3600000"
)

// rabbitmq params
const (
	rabbitmq_host = "amqp://guest:guest@broker:5672/"
)

const broker_to_use = "rabbitmq"

const parserUrl = "http://parser:9000"
const providersUrl = "http://find_providers:10000"

const db_to_use = "postgres"

var dbAPI *db.DB

var found_providers_lock *sync.Mutex
var found_providers map[string]time.Time

var requests_lock *sync.Mutex
var requests = 0

var count = 0

func incRequests() {
	requests_lock.Lock()
	defer requests_lock.Unlock()
	requests++
}

func decRequests() {
	requests_lock.Lock()
	defer requests_lock.Unlock()
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
	var waitfor = 50

	dbAPI = db.PrepareDB(db_to_use, pconf)

	requests_lock = new(sync.Mutex)
	found_providers_lock = new(sync.Mutex)
	found_providers = make(map[string]time.Time)
	cleanup := time.NewTicker(12 * time.Hour)
	reqsCh := make(chan struct{}, concurrency)
	provsCh := make(chan struct {
		timeOfReq time.Time
		timeNow   time.Time
		reqId     string
		ans       model.JsonAnswer
		err       error
	})

	logCh := broker.PrepareBroker(broker_to_use, rabbitmq_host, group_id)
	go fetchProviders(provsCh, parserUrl)

	requests := 0
	log.Infoln("Ready to go! concurrency:", concurrency, "batch:", batch)
	for {

		if batch > 0 && count >= batch {
			requests_lock.Lock()
			if requests > waitfor {
				log.Debug("----------------- waiting ----------------")
				<-time.After(10 * time.Second)
			}
			count = 0
			requests_lock.Unlock()

		}

		log.Debug("-------------------- Requests:", requests)
		select {
		case entry := <-logCh:
			reqsCh <- struct{}{}
			e, err := parseEntry(parserUrl, entry)
			reqId := genReqId(e)
			if err != nil {
				log.Warning("Error on parsing log entry:", entry, err)
				<-reqsCh
			} else {
				requests++
				go dbAPI.WriteEntryToDB(e, reqId)
				if *dontFindProviders {
					<-reqsCh
				} else {
					if !foundProviders(e.Cid) {
						incRequests()
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
			cleanupFoundProviders()
		}
	}

}

func cleanupFoundProviders() {
	found_providers_lock.Lock()
	defer found_providers_lock.Unlock()
	for p, t := range found_providers {
		if time.Now().After(t.Add(24 * time.Hour)) {
			delete(found_providers, p)
		}
	}
}

func foundProviders(cid string) bool {
	found_providers_lock.Lock()
	defer found_providers_lock.Unlock()
	t, ok := found_providers[cid]
	if ok && time.Now().After(t.Add(24*time.Hour)) {
		return false
	}
	return ok
}

func genReqId(e model.EntryStruct) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v%v%v%v%v%v%v", e.Time, e.Ip, e.Cid, e.BodyBytes, e.RequestTime, e.RequestLength, e.HttpUserAgent)))
	return string(h.Sum(nil))
}

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

func foundProvider(cid string) bool {
	found_providers_lock.Lock()
	defer found_providers_lock.Unlock()
	t, ok := found_providers[cid]
	if !ok || time.Now().After(t.Add(24*time.Hour)) {
		found_providers[cid] = time.Now()
		return true
	}
	return false
}

func parseEntry(url string, entry string) (model.EntryStruct, error) {
	resp := service.SendRequest("POST", fmt.Sprintf("%v/parse", url), "text/plain; charset=utf-8", bytes.NewBuffer([]byte(entry)))
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var e model.EntryStruct
	if resp.Status != "200 OK" {
		var err_msg struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &err_msg)
		return e, errors.New(fmt.Sprintf("%v: %v", resp.Status, err_msg.Error))
	}
	err := json.Unmarshal(bodyBytes, &e)
	if err != nil {
		panic(err)
	}

	return e, nil
}

func findAllProvider(url string, cid string) (model.JsonAnswer, error) {
	resp := service.SendRequest("GET", fmt.Sprintf("%v/findAllProviders/%v", url, cid), "", nil)

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans model.JsonAnswer
	if resp.Status != "200 OK" {
		var err_msg struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &err_msg)
		return ans, errors.New(fmt.Sprintf("%v: %v", resp.Status, err_msg.Error))
	}
	err := json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans, nil
}

func findProvider(url string, cid string) (model.JsonAnswer, error) {
	resp := service.SendRequest("GET", fmt.Sprintf("%v/findProviders/%v", url, cid), "", nil)

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans model.JsonAnswer
	if resp.Status != "200 OK" {
		var err_msg struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &err_msg)
		return ans, errors.New(fmt.Sprintf("%v: %v", resp.Status, err_msg.Error))
	}
	err := json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans, nil
}

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
