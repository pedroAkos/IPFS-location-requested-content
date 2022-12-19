package providers

import (
	"context"
	"find_providers/pkg/model"
	cid2 "github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"time"
)

// FindAllOf finds all providers of a given CID
func FindAllOf(cid cid2.Cid, kad *dht.IpfsDHT) []model.ProviderInfo {
	providers := make([]model.ProviderInfo, 0)
	start := time.Now()
	for p := range kad.FindProvidersAsync(context.Background(), cid, 0) {
		providers = append(providers, model.ProviderInfo{
			Provider: p,
			Dur:      time.Now().Sub(start),
		})
	}
	return providers
}
