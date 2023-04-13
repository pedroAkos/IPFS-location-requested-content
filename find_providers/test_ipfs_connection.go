package main

import (
	"context"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	log "github.com/sirupsen/logrus"
)

func main() {

	// Create a libp2p Host that listens on a random TCP port
	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	log.Println("My ID: ", h.ID().Pretty())

	canConnect := false
	// connect to the bootstrap nodes
	for _, pi := range dht.GetDefaultBootstrapPeerAddrInfos() {
		log.Println("Connecting to", pi.ID)
		if err := h.Connect(context.Background(), pi); err != nil {
			log.Println("Error connecting to:", err)
		} else {
			log.Println("Connected to:", pi.ID)
			canConnect = true
		}
	}

	if !canConnect {
		log.Println("Cannot connect to any bootstrap nodes, please troubleshoot")
	} else {
		log.Println("Connected to at least one bootstrap node, should be able to connect to the IPFS network")
	}
}
