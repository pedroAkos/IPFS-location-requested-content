package db

import (
	"context"
	"database/sql"
	"find_providers/pkg/model"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type DB struct {
	db_to_use string
	writeAPI  api.WriteAPIBlocking
	db        *sql.DB
}

type providerEntry struct {
	t    time.Time
	n    time.Time
	ans  model.JsonAnswer
	prov model.Provider
	locs model.Location
}

type dbWritable struct {
	toWrite string
	e       model.EntryStruct
	p       providerEntry
}

func PrepareDB(db_to_use string, conf Config) *DB {
	log.Debug("Preparing database..")

	db := &DB{
		db_to_use: db_to_use,
		writeAPI:  nil,
		db:        nil,
	}

	var err error
	switch db_to_use {
	case "postgres":
		pconf := conf.(PostgresConf)
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"dbname=%s sslmode=disable",
			pconf.Host, pconf.Port, pconf.User, pconf.DBname)

		log.Println("Opening connection to postgres database..")
		db.db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			panic(err)
		}
		err = db.db.Ping()
		if err != nil {
			panic(err)
		}

	case "influx":
		iconf := conf.(InfluxDBConf)
		log.Println("Opening connection to influxdb..")
		client := influxdb2.NewClient(iconf.DBUrl, iconf.Token)
		db.writeAPI = client.WriteAPIBlocking(iconf.Org, iconf.Bucket)
	}

	return db
}

func (db *DB) WriteEntryToDB(e model.EntryStruct, reqId string) {
	log.Debug("Writing to db request of cid", e.Cid)
	switch db.db_to_use {
	case "postgres":
		db.writeEntryToPostgres(e, reqId)
	case "influx":
		db.writeEntryToInfluxDB(e)
	}
}

func (db *DB) writeEntryToPostgres(e model.EntryStruct, reqId string) {
	sqlStatement := `INSERT INTO public.requests 
			(req_id, timestamp, cid, continent, country, region, lat, long, asn, aso,
			request_time, upstream_time,
			body_bytes, user_agent, cache, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			ON CONFLICT ON CONSTRAINT requests_pkey DO
			NOTHING 
			`
	_, err := db.db.Exec(sqlStatement, reqId, e.Time, e.Cid, checkIfValidString(e.Continent), checkIfValidString(e.Country), checkIfValidString(e.Region), checkIfValidFloat(e.Lat), checkIfValidFloat(e.Long), checkIfValidInt(e.ASN), checkIfValidString(e.ASO),
		checkIfValidFloat(e.RequestTime), checkIfValidFloat(e.UpstreamResponseTime[0]), checkIfValidFloat(e.BodyBytes), checkIfValidString(e.HttpUserAgent), checkIfValidString(e.Cache), checkIfValidInt(e.Status))
	if err != nil {
		log.Println(err, "on", e)
	}
}

func (db *DB) writeEntryToInfluxDB(e model.EntryStruct) {
	p := influxdb2.NewPoint("requests",
		map[string]string{"cid": e.Cid, "continent": e.Continent, "country": e.Country},
		map[string]interface{}{
			"regions":      e.Region,
			"request time": e.RequestTime, "upstream time": e.UpstreamResponseTime,
			"body bytes": e.BodyBytes, "user agent": e.HttpUserAgent, "cache": e.Cache},
		e.Time,
		//time.Now(),
	)
	err := db.writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		panic(err)
	}
}

func (db *DB) WriteProvidersToDB(t time.Time, n time.Time, ans model.JsonAnswer) {
	log.Debug("Writing to db providers of cid", ans.Cid)
	for _, prov := range ans.Providers {
		for _, locs := range prov.Locations {
			log.Println("Writing to db provider", prov.PeerId, " loc:", locs.Continent)
			switch db.db_to_use {
			case "postgres":
				db.writeProviderToPostgres(t, n, ans, prov, locs)
			case "influx":
				db.writeProviderToInfluxDB(t, n, ans, prov, locs)
			}
		}
	}
}

func (db *DB) writeProviderToPostgres(t time.Time, n time.Time, ans model.JsonAnswer, prov model.Provider, locs model.Location) {
	sqlStatement := `
			INSERT INTO public.providers
			(cid, continent, country, region, lat, long, asn, aso,
			request_time, peerID, found_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT ON CONSTRAINT providers_pkey DO 
   			UPDATE SET continent=COALESCE(NULLIF($2, ''), providers.continent),
   			    country=COALESCE(NULLIF($3, ''), providers.country),
   			    region=COALESCE(NULLIF($4, ''), providers.region),
   			    lat=COALESCE(NULLIF($5, NULL), providers.lat),
   			    long=COALESCE(NULLIF($6, NULL), providers.long),
   			    asn=COALESCE(NULLIF($7, NULL), providers.asn),
   			    aso=COALESCE(NULLIF($8, ''), providers.aso),
   			    updated_at = $12
			`
	_, err := db.db.Exec(sqlStatement, ans.Cid, checkIfValidString(locs.Continent), checkIfValidString(locs.Country), checkIfValidString(locs.Region), checkIfValidFloat(locs.Lat), checkIfValidFloat(locs.Long), checkIfValidInt(locs.ASN), checkIfValidString(locs.ASO),
		ans.Dur, checkIfValidString(strings.Trim(prov.PeerId, "{}")), n, n)
	if err != nil {
		log.Println(err, "on", ans.Cid, locs.Continent, locs.Country, locs.Region, locs.Lat, locs.Long, locs.ASN, locs.ASO,
			ans.Dur, prov.PeerId)
	}
}

func (db *DB) writeProviderToInfluxDB(t time.Time, n time.Time, ans model.JsonAnswer, prov model.Provider, locs model.Location) {
	p := influxdb2.NewPoint("providers",
		map[string]string{"cid": ans.Cid, "continent": locs.Continent, "country": locs.Country},
		map[string]interface{}{"peerID": prov.PeerId,
			"lat":          locs.Lat,
			"Long":         locs.Long,
			"request time": ans.Dur.Milliseconds(), "request at": n},
		t,
	)
	err := db.writeAPI.WritePoint(context.Background(), p)
	if err != nil {
		panic(err)
	}
}
