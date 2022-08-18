package main

import (
	"context"
	"encoding/json"
	"find_providers/pkg/data"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	cid2 "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"log"
	"net/http"
	"time"
)

var kad *dht.IpfsDHT

func main() {
	//logging.SetAllLoggers(logging.LevelDebug)

	port := flag.Int("port", 10000, "Port of the service")
	flag.Parse()

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

	kad, err = dht.New(context.Background(), h, dht.Mode(dht.ModeClient))
	if err != nil {
		panic(err)
	}

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/findProviders/{cid}", findProviders)

	log.Println("Running on port ", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), router))
}

func findProviders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cidStr := vars["cid"]
	if cid, err := cid2.Decode(cidStr); err != nil {
		http.Error(w, err.Error(), 400)

	} else {
		log.Println("Finding providers of cid", cidStr)
		start := time.Now()
		p, e := kad.FindProviders(context.Background(), cid)
		dur := time.Now().Sub(start)
		if e != nil {
			http.Error(w, e.Error(), 400)
		} else {
			ans := data.JsonAnswer{
				Cid:       cidStr,
				Providers: make([]data.Provider, len(p)),
				Dur:       dur,
			}

			for i, _p := range p {
				pstr := data.Provider{
					PeerId: _p.ID.Pretty(),
					MAddrs: make([]string, len(_p.Addrs)),
				}

				for j, _m := range _p.Addrs {
					pstr.MAddrs[j] = _m.String()
				}
				ans.Providers[i] = pstr
			}

			w.WriteHeader(200)
			_ = json.NewEncoder(w).Encode(ans)
			log.Println("Resolved providers of cid", cidStr, "duration:", ans.Dur)
		}
	}
}
