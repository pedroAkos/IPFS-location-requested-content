package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"io"
	"strconv"

	"bytes"
	"context"
	"encoding/json"
	"errors"
	"find_providers/pkg/data"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

//postgres params
const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = ""
	dbname   = "ipfs_content_location"
)

//influxdb params
const (
	org    = "my-org"
	bucket = "my-bucket"
	dbUrl  = "http://localhost:8086"
	token  = "my-super-secret-auth-token"
)

//kafka params
const (
	bootstrap_servers    = "127.0.0.1:9092"
	group_id             = "ipfs-gateway-logs"
	max_poll_interval_ms = "3600000"
)

const db_to_use = "postgres"

var writeAPI api.WriteAPIBlocking
var db *sql.DB

type providerEntry struct {
	t    time.Time
	n    time.Time
	ans  data.JsonAnswer
	prov data.Provider
	locs data.Location
}

type dbWritable struct {
	toWrite string
	e       data.EntryStruct
	p       providerEntry
}

func main() {
	var err error
	concurrency := 100

	prepareDB()
	consumer := prepareKafka()

	parserUrl := "http://127.0.0.1:9000"
	providersUrl := "http://127.0.0.1:9001"

	logCh := make(chan string)
	reqsCh := make(chan struct{}, concurrency)
	provsCh := make(chan struct {
		timeOfReq time.Time
		timeNow   time.Time
		ans       data.JsonAnswer
		err       error
	})

	go consumeLog(consumer, logCh)

	requests := 0
	for {
		log.Println("-------------------- Requests:", requests)
		select {
		case entry := <-logCh:
			reqsCh <- struct{}{}
			e, err := parseEntry(parserUrl, entry)
			if err != nil {
				log.Println("Error on parsing log entry:", entry, err)
				<-reqsCh
			} else {
				requests++
				go writeEntryToDB(e)
				go func(url string, cid string, t time.Time) {
					ans := struct {
						timeOfReq time.Time
						timeNow   time.Time
						ans       data.JsonAnswer
						err       error
					}{timeOfReq: t, timeNow: time.Now()}
					a, e := findProvider(url, cid)
					ans.ans = a
					ans.err = e
					<-reqsCh
					provsCh <- ans
				}(providersUrl, e.Cid, e.Time)
			}
		case providers := <-provsCh:
			requests--
			if providers.err != nil {
				log.Println("Error on fetching providers:", providers.err)
			} else {
				go func(url string, timeOfReq time.Time, timeNow time.Time, ans data.JsonAnswer) {
					ans.Providers, err = parseProviders(url, ans.Providers)
					if err != nil {
						log.Println("Error on parsing providers:", err)
					} else {
						writeProvidersToDB(providers.timeOfReq, providers.timeNow, providers.ans)
					}
				}(parserUrl, providers.timeOfReq, providers.timeNow, providers.ans)
			}
		}

	}
}

func prepareKafka() *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    bootstrap_servers,
		"group.id":             group_id,
		"max.poll.interval.ms": max_poll_interval_ms,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "smallest"}})

	if err != nil {
		panic(err)
	}

	err = consumer.Subscribe("logs", nil)
	if err != nil {
		panic(err)

	}

	return consumer
}

func prepareDB() {
	var err error
	switch db_to_use {
	case "postgres":
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"dbname=%s sslmode=disable",
			host, port, user, dbname)

		db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		err = db.Ping()
		if err != nil {
			panic(err)
		}

	case "influx":
		client := influxdb2.NewClient(dbUrl, token)
		writeAPI = client.WriteAPIBlocking(org, bucket)
	}
}

func writeEntryToDB(e data.EntryStruct) {
	log.Println("Writing to db request of cid", e.Cid)
	switch db_to_use {
	case "postgres":
		writeEntryToPostgres(e)
	case "influx":
		writeEntryToInfluxDB(e)
	}
}

func checkIfValidString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	} else {
		return sql.NullString{
			String: s,
			Valid:  true,
		}
	}
}

func checkIfValidFloat(s string) sql.NullFloat64 {
	if len(s) == 0 {
		return sql.NullFloat64{}
	} else {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return sql.NullFloat64{}
		}
		return sql.NullFloat64{
			Float64: f,
			Valid:   true,
		}
	}
}

func writeEntryToPostgres(e data.EntryStruct) {
	sqlStatement := `INSERT INTO public.requests 
			(timestamp, cid, continent, country, lat, long,
			request_time, upstream_time,
			body_bytes, user_agent, cache)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			`
	_, err := db.Exec(sqlStatement, e.Time, e.Cid, checkIfValidString(e.Continent), checkIfValidString(e.Country), checkIfValidFloat(e.Lat), checkIfValidFloat(e.Long),
		checkIfValidFloat(e.RequestTime), checkIfValidFloat(e.UpstreamResponseTime[0]), checkIfValidFloat(e.BodyBytes), checkIfValidString(e.HttpUserAgent), checkIfValidString(e.Cache))
	if err != nil {
		panic(err)
	}
}

func writeEntryToInfluxDB(e data.EntryStruct) {
	p := influxdb2.NewPoint("requests",
		map[string]string{"cid": e.Cid, "continent": e.Continent, "country": e.Country},
		map[string]interface{}{
			"regions":      e.Regions,
			"request time": e.RequestTime, "upstream time": e.UpstreamResponseTime,
			"body bytes": e.BodyBytes, "user agent": e.HttpUserAgent, "cache": e.Cache},
		e.Time,
		//time.Now(),
	)
	err := writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		panic(err)
	}
}

func writeProvidersToDB(t time.Time, n time.Time, ans data.JsonAnswer) {
	log.Println("Writing to db providers of cid", ans.Cid)
	for _, prov := range ans.Providers {
		for _, locs := range prov.Locations {
			switch db_to_use {
			case "postgres":
				writeProviderToPostgres(t, n, ans, prov, locs)
			case "influx":
				writeProviderToInfluxDB(t, n, ans, prov, locs)
			}
		}
	}
}

func writeProviderToPostgres(t time.Time, n time.Time, ans data.JsonAnswer, prov data.Provider, locs data.Location) {
	sqlStatement := `
			INSERT INTO public.providers
			(timestamp, cid, continent, country, lat, long,
			request_time, peerID, request_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`
	_, err := db.Exec(sqlStatement, t, ans.Cid, checkIfValidString(locs.Continent), checkIfValidString(locs.Country), checkIfValidFloat(locs.Lat), checkIfValidFloat(locs.Long),
		ans.Dur, checkIfValidString(prov.PeerId), n)
	if err != nil {
		panic(err)
	}
}

func writeProviderToInfluxDB(t time.Time, n time.Time, ans data.JsonAnswer, prov data.Provider, locs data.Location) {
	p := influxdb2.NewPoint("providers",
		map[string]string{"cid": ans.Cid, "continent": locs.Continent, "country": locs.Country},
		map[string]interface{}{"peerID": prov.PeerId,
			"lat":          locs.Lat,
			"Long":         locs.Long,
			"request time": ans.Dur.Milliseconds(), "request at": n},
		t,
	)
	err := writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		panic(err)
	}
}

func consumeLog(consumer *kafka.Consumer, logCh chan string) {
	run := true
	for run == true {
		ev := consumer.Poll(10000)
		switch e := ev.(type) {
		case *kafka.Message:
			logCh <- string(e.Value)
		case kafka.Error:
			_, _ = fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			//fmt.Printf("Ignored %v\n", e)
		}
	}
}

func sendRequest(method string, url string, contentType string, body io.Reader) *http.Response {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(err)
	}
	req.Close = true
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

func parseEntry(url string, entry string) (data.EntryStruct, error) {
	resp := sendRequest("POST", fmt.Sprintf("%v/parse", url), "text/plain; charset=utf-8", bytes.NewBuffer([]byte(entry)))
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var e data.EntryStruct
	if resp.Status != "200 OK" {
		return e, errors.New(resp.Status)
	}
	err := json.Unmarshal(bodyBytes, &e)
	if err != nil {
		panic(err)
	}

	return e, nil
}

func findProvider(url string, cid string) (data.JsonAnswer, error) {
	resp := sendRequest("GET", fmt.Sprintf("%v/findProviders/%v", url, cid), "", nil)

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans data.JsonAnswer
	if resp.Status != "200 OK" {
		return ans, errors.New(resp.Status)
	}
	err := json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans, nil
}

func parseProviders(url string, providers []data.Provider) ([]data.Provider, error) {
	providersJson, _ := json.Marshal(providers)

	resp := sendRequest("POST", fmt.Sprintf("%v/locate_providers", url), "application/json; charset=utf-8", bytes.NewBuffer(providersJson))

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
