package data

import "time"

type Location struct {
	Continent string  `json:"continent"`
	Country   string  `json:"country"`
	Lat       float64 `json:"lat"`
	Long      float64 `json:"long"`
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