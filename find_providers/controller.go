package main

import (
	"crypto/sha256"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"io"
	"strconv"
	"sync"
	"unicode/utf8"

	"bytes"
	"context"
	"encoding/json"
	"errors"
	"find_providers/pkg/data"
	"fmt"
	// "github.com/confluentinc/confluent-kafka-go/kafka"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

//postgres params
const (
	host     = "db"
	port     = 5432
	user     = "postgres"
	password = ""
	dbname   = "ipfs_content_location"
)

//influxdb params
const (
	org    = "my-org"
	bucket = "my-bucket"
	dbUrl  = "http://db:8086"
	token  = "my-super-secret-auth-token"
)

//kafka params
const (
	bootstrap_servers    = "kafka:9092"
	group_id             = "ipfs-gateway-logs"
	max_poll_interval_ms = "3600000"
)

//rabbitmq params
const (
	rabbitmq_host = "amqp://guest:guest@broker:5672/"
)

const broker_to_use = "rabbitmq"

const parserUrl = "http://parser:9000"
const providersUrl = "http://find_providers:10000"

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

var found_providers_lock *sync.Mutex
var found_providers map[string]time.Time

var requests_lock *sync.Mutex
var requests = 0

func incRequests() {
	requests_lock.Lock()
	defer requests_lock.Unlock()
	requests++
}

func decRequests() {
	requests_lock.Lock()
	defer requests_lock.Unlock()
	requests--
}

func main() {
	//var err error
	concurrency := 100

	prepareDB()

	requests_lock = new(sync.Mutex)
	found_providers_lock = new(sync.Mutex)
	found_providers = make(map[string]time.Time)
	logCh := make(chan string)
	reqsCh := make(chan struct{}, concurrency)
	provsCh := make(chan struct {
		timeOfReq time.Time
		timeNow   time.Time
		reqId     string
		ans       data.JsonAnswer
		err       error
	})

	go prepareBroker(logCh)
	go fetchProviders(provsCh, parserUrl)

	//requests := 0
	log.Println("Ready to go!")
	for {
		log.Println("-------------------- Requests:", requests)
		select {
		case entry := <-logCh:
			reqsCh <- struct{}{}
			e, err := parseEntry(parserUrl, entry)
			reqId := genReqId(e)
			if err != nil {
				log.Println("Error on parsing log entry:", entry, err)
				<-reqsCh
			} else {
				//requests++
				incRequests()
				go writeEntryToDB(e, reqId)
				if !foundProviders(e.Cid) {
					go func(url string, cid string, t time.Time) {
						ans := struct {
							timeOfReq time.Time
							timeNow   time.Time
							reqId     string
							ans       data.JsonAnswer
							err       error
						}{timeOfReq: t, timeNow: time.Now(), reqId: reqId}
						a, e := findProvider(url, cid)
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
			//case providers := <-provsCh:
			//	requests--
			//	if providers.err != nil {
			//		log.Println("Error on fetching providers:", providers.err)
			//	} else {
			//		go func(url string, timeOfReq time.Time, timeNow time.Time, ans data.JsonAnswer) {
			//			ans.Providers, err = parseProviders(url, ans.Providers)
			//			if err != nil {
			//				log.Println("Error on parsing providers:", err)
			//			} else {
			//				writeProvidersToDB(providers.timeOfReq, providers.timeNow, providers.ans)
			//			}
			//		}(parserUrl, providers.timeOfReq, providers.timeNow, providers.ans)
			//	}
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

func prepareBroker(logch chan string) {

	switch broker_to_use {
	case "kafka":
		log.Println("Preparing kafka broker..")
		//consumer := prepareKafka()
		//consumeKafkaLog(consumer, logch)
	case "rabbitmq":
		log.Println("Preparing rabbitmq broker..")
		msgs := prepareRabbitMq()
		consumeRabbitMq(msgs, logch)

	}

}

func consumeRabbitMq(msgs <-chan amqp.Delivery, logch chan string) {
	for m := range msgs {
		logch <- string(m.Body)
	}
}

func prepareRabbitMq() <-chan amqp.Delivery {
	conn, err := amqp.Dial(rabbitmq_host)
	if err != nil {
		panic(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	q, err := ch.QueueDeclare(
		group_id,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	return msgs
}

func genReqId(e data.EntryStruct) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v%v%v%v%v%v%v", e.Time, e.Ip, e.Cid, e.BodyBytes, e.RequestTime, e.RequestLength, e.HttpUserAgent)))
	return string(h.Sum(nil))
}

func fetchProviders(provsCh chan struct {
	timeOfReq time.Time
	timeNow   time.Time
	reqId     string
	ans       data.JsonAnswer
	err       error
}, parserUrl string) {

	var err error
	for {
		providers := <-provsCh
		//requests--
		decRequests()
		if providers.err != nil {
			log.Println("Error on fetching providers:", providers.err)
		} else {
			if len(providers.ans.Providers) > 0 && foundProvider(providers.ans.Cid) {
				go func(url string, timeOfReq time.Time, timeNow time.Time, ans data.JsonAnswer, reqId string) {
					ans.Providers, err = parseProviders(url, ans.Providers)
					if err != nil {
						log.Println("Error on parsing providers:", err)
					} else {
						writeProvidersToDB(providers.timeOfReq, providers.timeNow, providers.ans)
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

//func prepareKafka() *kafka.Consumer {
//	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
//		"bootstrap.servers":    bootstrap_servers,
//		"group.id":             group_id,
//		"max.poll.interval.ms": max_poll_interval_ms,
//		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "smallest"}})
//
//	if err != nil {
//		panic(err)
//	}
//
//	err = consumer.Subscribe("logs", nil)
//	if err != nil {
//		panic(err)
//
//	}
//
//	return consumer
//}

func prepareDB() {
	log.Println("Preparing database..")
	var err error
	switch db_to_use {
	case "postgres":
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"dbname=%s sslmode=disable",
			host, port, user, dbname)

		log.Println("Opening connection to postgres database..")
		db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		err = db.Ping()
		if err != nil {
			panic(err)
		}

	case "influx":
		log.Println("Opening connection to influxdb..")
		client := influxdb2.NewClient(dbUrl, token)
		writeAPI = client.WriteAPIBlocking(org, bucket)
	}
}

func writeEntryToDB(e data.EntryStruct, reqId string) {
	log.Println("Writing to db request of cid", e.Cid)
	switch db_to_use {
	case "postgres":
		writeEntryToPostgres(e, reqId)
	case "influx":
		writeEntryToInfluxDB(e)
	}
}

func checkIfValidString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	} else {
		if !utf8.Valid([]byte(s)) {
			panic(fmt.Sprintf("String %v is not valid utf8", s))
		}
		return sql.NullString{
			String: s,
			Valid:  true,
		}
	}
}

func checkIfValidInt(s string) sql.NullInt32 {
	if len(s) == 0 {
		return sql.NullInt32{}
	} else {
		i, err := strconv.Atoi(s)
		if err != nil {
			return sql.NullInt32{}
		}
		return sql.NullInt32{
			Int32: int32(i),
			Valid: true,
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

func writeEntryToPostgres(e data.EntryStruct, reqId string) {
	sqlStatement := `INSERT INTO public.requests 
			(req_id, timestamp, cid, continent, country, region, lat, long, asn, aso,
			request_time, upstream_time,
			body_bytes, user_agent, cache)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			ON CONFLICT ON CONSTRAINT requests_pkey DO
			NOTHING 
			`
	_, err := db.Exec(sqlStatement, reqId, e.Time, e.Cid, checkIfValidString(e.Continent), checkIfValidString(e.Country), checkIfValidString(e.Region), checkIfValidFloat(e.Lat), checkIfValidFloat(e.Long), checkIfValidInt(e.ASN), checkIfValidString(e.ASO),
		checkIfValidFloat(e.RequestTime), checkIfValidFloat(e.UpstreamResponseTime[0]), checkIfValidFloat(e.BodyBytes), checkIfValidString(e.HttpUserAgent), checkIfValidString(e.Cache))
	if err != nil {
		log.Println(err, "on", e)
	}
}

func writeEntryToInfluxDB(e data.EntryStruct) {
	p := influxdb2.NewPoint("requests",
		map[string]string{"cid": e.Cid, "continent": e.Continent, "country": e.Country},
		map[string]interface{}{
			"regions":      e.Region,
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
			(cid, continent, country, region, lat, long, asn, aso,
			request_time, peerID, found_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT ON CONSTRAINT providers_pkey DO 
   			UPDATE SET updated_at = $12
			`
	_, err := db.Exec(sqlStatement, ans.Cid, checkIfValidString(locs.Continent), checkIfValidString(locs.Country), checkIfValidString(locs.Region), checkIfValidFloat(locs.Lat), checkIfValidFloat(locs.Long), checkIfValidInt(locs.ASN), checkIfValidString(locs.ASO),
		ans.Dur, checkIfValidString(prov.PeerId), n, n)
	if err != nil {
		log.Println(err, "on", ans.Cid, locs.Continent, locs.Country, locs.Region, locs.Lat, locs.Long, locs.ASN, locs.ASO,
			ans.Dur, prov.PeerId)
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

//func consumeKafkaLog(consumer *kafka.Consumer, logCh chan string) {
//	run := true
//	for run == true {
//		ev := consumer.Poll(10000)
//		switch e := ev.(type) {
//		case *kafka.Message:
//			logCh <- string(e.Value)
//		case kafka.Error:
//			_, _ = fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
//			run = false
//		default:
//			//fmt.Printf("Ignored %v\n", e)
//		}
//	}
//}

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

func findProvider(url string, cid string) (data.JsonAnswer, error) {
	resp := sendRequest("GET", fmt.Sprintf("%v/findProviders/%v", url, cid), "", nil)

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans data.JsonAnswer
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
