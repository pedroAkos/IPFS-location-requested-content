package data

import "time"

type Location struct {
	ASN       string `json:"asn"`
	ASO       string `json:"aso"`
	Continent string `json:"continent"`
	Country   string `json:"country"`
	Lat       string `json:"lat"`
	Long      string `json:"long"`
	Region    string `json:"region"`
}

type Provider struct {
	PeerId    string     `json:"peerId"`
	MAddrs    []string   `json:"maddrs"`
	Locations []Location `json:"locations"`
}

type JsonAnswer struct {
	Cid       string        `json:"cid"`
	Providers []Provider    `json:"providers"`
	Dur       time.Duration `json:"duration"`
}
