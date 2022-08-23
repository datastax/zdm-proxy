package cloudgateproxy

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"time"
)

type ContactInfo struct {
	TypeName        string   `json:"type"`
	LocalDc         string   `json:"local_dc"`
	SniProxyAddress string   `json:"sni_proxy_address"`
	ContactPoints   []string `json:"contact_points"`
}

type AstraMetadata struct {
	Version     int         `json:"version"`
	Region      string      `json:"region"`
	ContactInfo ContactInfo `json:"contact_info"`
}

const AstraMetadataHttpTimeout = 30 * time.Second

func retrieveAstraMetadata(astraMetadataServiceHostName string, astraMetadataServicePort string,
	astraTlsConfig *tls.Config, ctx context.Context) (*AstraMetadata, error) {
	var metadata *AstraMetadata
	// create an HTTP Client using TLS to point to the metadata service
	//targetMetadataServiceUrl := "https://" + astraMetadataServiceHostName + ":" + astraMetadataServicePort + "/metadata"
	targetMetadataServiceUrl := fmt.Sprintf("https://%s:%s/metadata", astraMetadataServiceHostName, astraMetadataServicePort)
	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: astraTlsConfig,
		},
		Timeout: AstraMetadataHttpTimeout,
	}

	// Issue HTTPS request (client.Get("/metadata")) to MetadataService to discover contact points (Stargates).
	req, err := http.NewRequestWithContext(ctx, "GET", targetMetadataServiceUrl, nil)
	if err != nil {
		log.Errorf("Failed to create metadata HTTP request to %v due to %v", targetMetadataServiceUrl, err)
		return nil, err
	}

	metadataResponse, err := httpsClient.Do(req)
	if err != nil {
		log.Errorf("Failed to retrieve the target metadata information from %s due to %v", targetMetadataServiceUrl, err)
		return nil, err
	}

	metadataBody, err := ioutil.ReadAll(metadataResponse.Body)
	log.Debugf("Metadata JSON: %s", string(metadataBody))

	if metadataResponse.StatusCode < 200 || metadataResponse.StatusCode >= 300 {
		return nil, fmt.Errorf("metadata service (Astra) returned not successful status code %d, body: %v",
			metadataResponse.StatusCode, string(metadataBody))
	}

	err = json.Unmarshal(metadataBody, &metadata)
	return metadata, err
}
