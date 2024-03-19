package main

import (
	"context"
	"flag"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	//logging.SetAllLoggers(logging.LevelDebug)
	file := flag.String("f", "", "File with content to get")
	resolvePeers := flag.Bool("resPeers", false, "Resolve peers instead of cids")
	concurrency = flag.Int("c", 5, "Number of concurrent requests")
	waitTime := flag.Duration("w", time.Minute*0, "Grace period before workload")
	logfile := flag.String("out", "", "Output file")
	timeout = flag.Duration("timeout", time.Minute*0, "Query timeout")

	progress = flag.Bool("progress", false, "Show progress bar")

	flag.Parse()
	if f, err := os.Open(*file); err != nil {
		panic(err)
	} else {
		ncids, err = lineCounter(f)
		if err != nil {
			panic(err)
		}
		_ = f.Close()
	}

	// Create a libp2p Host that listens on a random TCP port
	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	log.Println("My ID: ", h.ID().Pretty())

	// connect to the bootstrap nodes
	for _, pi := range dht.GetDefaultBootstrapPeerAddrInfos() {
		log.Println("Connecting to", pi.ID)
		if err := h.Connect(context.Background(), pi); err != nil {
			log.Println("Error connecting to:", err)
		} else {
			log.Println("Connected to:", pi.ID)
		}
	}

	// Create a DHT, using a random peer ID.
	kad, err := dht.New(context.Background(), h, dht.Mode(dht.ModeClient))
	if err != nil {
		panic(err)
	}

	// Bootstrap the DHT. In the default configuration, this spawns a Background
	err = kad.Bootstrap(context.Background())
	if err != nil {
		panic(err)
	}

	if *logfile != "" {
		f, err := os.Create(*logfile)
		if err != nil {
			panic(err)
		}
		log.SetOutput(f)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT)

	select {
	case <-time.After(*waitTime):
		start := time.Now()
		log.Println("Beginning workload")
		startWorkload(kad, *file, *resolvePeers)
		log.Println("Finished workload", "time", time.Now().Sub(start))
	case <-stop:
		os.Exit(0)
	}
}
