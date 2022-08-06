package main

import (
	"bytes"
	"encoding/json"
	"find_providers/pkg/data"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

func main() {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    "host1:9092,host2:9092",
		"group.id":             "foo",
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "smallest"}})

	parserUrl := "url"
	providersUrl := "url"

	if err != nil {
		panic(err)
	}

	logCh := make(chan string)
	reqsCh := make(chan struct{}, *concurrency)
	provsCh := make(chan struct {
		timeOfReq time.Time
		timeNow   time.Time
		ans       data.JsonAnswer
	})

	go consumeLog(consumer, logCh)

	for {
		select {
		case reqsCh <- struct{}{}:
		default:
			select {
			case entry := <-logCh:
				e := parseEntry(parserUrl, entry)
				go writeEntryToDB(e)
				go func(url string, cid string, t time.Time) {
					provsCh <- struct {
						timeOfReq time.Time
						timeNow   time.Time
						ans       data.JsonAnswer
					}{timeOfReq: t, timeNow: time.Now(), ans: findProvider(url, cid)}
					<-reqsCh
				}(providersUrl, e.Cid, e.Time)
			case providers := <-provsCh:
				go func(url string, timeOfReq time.Time, timeNow time.Time, ans data.JsonAnswer) {
					ans.Providers = parseProviders(url, ans.Providers)
					writeProvidersToDB(providers.timeOfReq, providers.timeNow, providers.ans)
				}(parserUrl, providers.timeOfReq, providers.timeNow, providers.ans)
			}
		}

	}
}

func writeEntryToDB(e data.EntryStruct) {

}

func writeProvidersToDB(t time.Time, n time.Time, ans data.JsonAnswer) {

}

func consumeLog(consumer *kafka.Consumer, logCh chan string) {
	run := true
	for run == true {
		ev := consumer.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			logCh <- string(e.Value)
		case kafka.Error:
			_, _ = fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
}

func parseEntry(url string, entry string) data.EntryStruct {
	resp, err := http.Post(url, "text/plain; charset=utf-8", bytes.NewBuffer([]byte(entry)))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var e data.EntryStruct
	err = json.Unmarshal(bodyBytes, &e)
	if err != nil {
		panic(err)
	}

	return e
}

func findProvider(url string, cid string) data.JsonAnswer {
	resp, err := http.Get(fmt.Sprintf("%v/%v", url, cid))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var ans data.JsonAnswer
	err = json.Unmarshal(bodyBytes, &ans)
	if err != nil {
		panic(err)
	}

	return ans
}

func parseProviders(url string, providers []data.Provider) []data.Provider {

	providersJson, _ := json.Marshal(providers)

	resp, err := http.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(providersJson))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	err = json.Unmarshal(bodyBytes, &providers)
	if err != nil {
		panic(err)
	}

	return providers
}
