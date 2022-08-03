package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	cid2 "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/schollz/progressbar/v3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var ncids = 0

var concurrency *int
var timeout *time.Duration

var progress *bool
var setniloutput = progressbar.OptionSetWriter(ioutil.Discard)

func main() {
	//logging.SetAllLoggers(logging.LevelDebug)
	file := flag.String("f", "", "File with content to get")
	concurrency = flag.Int("c", 5, "Number of concurrent requests")
	waitime := flag.Duration("w", time.Minute*0, "Grace period before workload")
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

	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	log.Println("My ID: ", h.ID().Pretty())

	for _, pi := range dht.GetDefaultBootstrapPeerAddrInfos() {
		log.Println("Connecting to", pi.ID)
		if err := h.Connect(context.Background(), pi); err != nil {
			log.Println("Error connecting to:", err)
		} else {
			log.Println("Connected to:", pi.ID)
		}
	}

	kad, err := dht.New(context.Background(), h, dht.Mode(dht.ModeClient))
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
	case <-time.After(*waitime):
		start := time.Now()
		log.Println("Beginning workload")
		startWorkload(kad, *file)
		log.Println("Finished workload", "time", time.Now().Sub(start))
	case <-stop:
		os.Exit(0)
	}
}

func startWorkload(kad *dht.IpfsDHT, file string) {
	if f, err := os.Open(file); err != nil {
		panic(err)
	} else {
		defer func(f *os.File) {
			_ = f.Close()
		}(f)

		getKeys(kad, f, *concurrency)
	}
}

func getKeys(kad *dht.IpfsDHT, f *os.File, concurrent int) {
	global := 0
	tokens := make(chan struct{}, concurrent)
	answers := make(chan answer)

	//Get cids from file
	cids := loadFile(bufio.NewReader(f), ncids)
	_ = f.Close()
	bar := progressbar.Default(int64(ncids))
	if !(*progress) {
		setniloutput(bar)
	}

	for _, cidStr := range cids {
		hasAnswer := true
		for hasAnswer { //get all pending answers
			select {
			case a := <-answers:
				_ = bar.Add(1)
				logAnswer(a)
				global--
			default:
				hasAnswer = false
			}
		}
		select {
		case tokens <- struct{}{}: //if can add token
		case a := <-answers: //or get another answer
			_ = bar.Add(1)
			logAnswer(a)
			global--
		}
		//continue
		searchCid(kad, cidStr, &global, tokens, answers)
	}
	for global > 0 { //if there are still pending answers wait for them
		_ = bar.Add(1)
		a := <-answers
		logAnswer(a)
		global--

	}
	_ = bar.Close()
}

func searchCid(kad *dht.IpfsDHT, cidStr string, global *int, tokens chan struct{}, answers chan answer) {
	if cid, err := cid2.Decode(cidStr); err != nil {
		log.Println("Error: ", err, " on decoding ", cidStr)
	} else {
		go func(cid cid2.Cid) {
			start := time.Now()
			providersCh := make(chan struct {
				providers []peer.AddrInfo
				err       error
			})
			opCtx := context.Background()
			var cancel context.CancelFunc = nil
			if *timeout > 0 {
				opCtx, cancel = context.WithTimeout(context.Background(), *timeout)
				defer cancel()
			}
			go func(cid cid2.Cid, opCtx context.Context) {
				p, e := kad.FindProviders(opCtx, cid)
				providersCh <- struct {
					providers []peer.AddrInfo
					err       error
				}{providers: p, err: e}
			}(cid, opCtx)

			select {
			case p := <-providersCh:
				answers <- answer{p: p.providers, err: p.err, cid: cid, dur: time.Now().Sub(start)}
			}
			<-tokens //try to remove 1 token
		}(cid)
		*global++
	}
}

func loadFile(reader *bufio.Reader, lines int) []string {
	//fmt.Fprintf(os.Stderr, "Loading File")
	cids := make([]string, lines)
	i := 0
	for {
		b, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		cidStr := string(b[:len(b)-1])
		cids[i] = cidStr
		i++
	}
	_, _ = fmt.Fprintf(os.Stderr, "Loaded File")
	return cids
}

type answer struct {
	p   []peer.AddrInfo
	cid cid2.Cid
	dur time.Duration
	err error
}

func logAnswer(a answer) {
	if a.err != nil {
		log.Println("Failed: ", a.cid, "err: ", a.err, " in peers: ", a.p, " time: ", a.dur)
	} else {
		log.Println("Found: ", a.cid, " in peers: ", a.p, " time: ", a.dur)
	}
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
