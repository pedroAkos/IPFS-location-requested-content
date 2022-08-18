package main

import (
	"database/sql"
	_ "github.com/lib/pq"

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
	password = "my-password"
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

func main() {
	var err error
	concurrency := 5

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

	for {
		select {
		case reqsCh <- struct{}{}:
		case providers := <-provsCh:
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
		select {
		case entry := <-logCh:
			e, err := parseEntry(parserUrl, entry)
			if err != nil {
				log.Println("Error on parsing log entry:", entry, err)
				select {
				case <-reqsCh:
				default:
				}
			} else {
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
					provsCh <- ans
					<-reqsCh
				}(providersUrl, e.Cid, e.Time)
			}
		case providers := <-provsCh:
			if providers.err != nil {
				log.Println("Error on fetching providers:", providers.err)
			} else {
				func(url string, timeOfReq time.Time, timeNow time.Time, ans data.JsonAnswer) {
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
			"password=%s dbname=%s sslmode=disable",
			host, port, user, password, dbname)

		db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		defer db.Close()
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

func writeEntryToPostgres(e data.EntryStruct) {
	sqlStatement := `
			INSERT INTO requests 
			(timestamp, cid, continent, country, lat, long,
			request_time, upstream_time,
			body_bytes, user_agent, cache)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			`
	err := db.QueryRow(sqlStatement, e.Time, e.Continent, e.Country, e.Lat, e.Long,
		e.RequestTime, e.UpstreamResponseTime, e.BodyBytes, e.HttpUserAgent, e.Cache)
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
			INSERT INTO providers
			(timestamp, cid, continent, country, lat, long,
			request_time, peerID, request_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`
	err := db.QueryRow(sqlStatement, t, locs.Continent, locs.Country, locs.Lat, locs.Long,
		ans.Dur, prov.PeerId, n)
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

func parseEntry(url string, entry string) (data.EntryStruct, error) {
	resp, err := http.Post(fmt.Sprintf("%v/parse", url), "text/plain; charset=utf-8", bytes.NewBuffer([]byte(entry)))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var e data.EntryStruct
	if resp.Status != "200 OK" {
		return e, errors.New(resp.Status)
	}
	err = json.Unmarshal(bodyBytes, &e)
	if err != nil {
		panic(err)
	}

	return e, nil
}

func findProvider(url string, cid string) (data.JsonAnswer, error) {
	resp, err := http.Get(fmt.Sprintf("%v/findProviders/%v", url, cid))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans data.JsonAnswer
	if resp.Status != "200 OK" {
		return ans, errors.New(resp.Status)
	}
	err = json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans, nil
}

func parseProviders(url string, providers []data.Provider) ([]data.Provider, error) {

	providersJson, _ := json.Marshal(providers)

	resp, err := http.Post(fmt.Sprintf("%v/locate_providers", url), "application/json; charset=utf-8", bytes.NewBuffer(providersJson))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.Status != "200 OK" {
		return providers, errors.New(resp.Status)
	}

	err = json.Unmarshal(bodyBytes, &providers)
	if err != nil {
		panic(err)
	}

	return providers, nil
}
