package model

import (
	"time"
)

type EntryStruct struct {
	ASN                  string    `json:"asn"`
	ASO                  string    `json:"aso"`
	BodyBytes            string    `json:"body_bytes"`
	Cache                string    `json:"cache"`
	Cid                  string    `json:"cid"`
	Continent            string    `json:"continent"`
	Country              string    `json:"country"`
	Lat                  string    `json:"lat"`
	Long                 string    `json:"long"`
	Http                 string    `json:"http"`
	HttpHost             string    `json:"http_host"`
	HttpRefer            string    `json:"http_refer"`
	HttpUserAgent        string    `json:"http_user_agent"`
	Ip                   string    `json:"ip"`
	Op                   string    `json:"op"`
	Region               string    `json:"region"`
	RequestLength        string    `json:"request_length"`
	RequestTime          string    `json:"request_time"`
	Scheme               string    `json:"scheme"`
	ServerName           string    `json:"server_name"`
	Status               string    `json:"status"`
	Target               string    `json:"target"`
	Time                 time.Time `json:"time"`
	UpstreamHeaderTime   []string  `json:"upstream_header_time"`
	UpstreamResponseTime []string  `json:"upstream_response_time"`
}
