package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"find_providers/pkg/broker"
	"find_providers/pkg/db"
	"find_providers/pkg/model"
	"find_providers/pkg/service"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

var conf = db.PostgresConf{
	Host:     "db",
	Port:     5432,
	User:     "postgres",
	Password: "",
	DBname:   "ipfs_content_location",
}

func main() {
	dbAPI := db.PrepareDB("postgres", conf)
	logCh := broker.PrepareBroker("rabbitmq", "amqp://guest:guest@broker:5672/", "providers")

	for {
		entry := <-logCh
		ans, err := parseFindProvidersEntry("http://parser:9000", entry)
		if err != nil {
			log.Warning("Error on parsing log entry:", entry, err)
			continue
		}
		t, err := time.Parse("2022/06/01 12:00:00", ans.Time)
		if err != nil {
			log.Warning("Error parsing time")
			t = time.Now()
		}
		dbAPI.WriteProvidersToDB(t, time.Now(), ans)

	}

}

func parseFindProvidersEntry(url string, entry string) (model.JsonAnswer, error) {
	resp := service.SendRequest("POST", fmt.Sprintf("%v/parse/findProvidersLog", url), "text/plain; charset=utf-8", bytes.NewBuffer([]byte(entry)))
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
