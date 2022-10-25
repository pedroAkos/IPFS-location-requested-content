package model

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"time"
)

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
	Time      string        `json:"time"`
	Cid       string        `json:"cid"`
	Providers []Provider    `json:"providers"`
	Dur       time.Duration `json:"duration"`
}

type ProviderInfo struct {
	Provider peer.AddrInfo
	Dur      time.Duration
}
