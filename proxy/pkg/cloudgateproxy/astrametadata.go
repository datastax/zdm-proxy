package cloudgateproxy

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

type ContactInfo struct {
	TypeName string `json:"type"`
	LocalDc string `json:"local_dc"`
	SniProxyAddress string `json:"sni_proxy_address"`
	RoundRobinPort int `json:"round_robin_port"`
	ContactPoints []string `json:"contact_points"`
}

type AstraMetadata struct {
	Version int `json:"version"`
	Region string `json:"region"`
	ContactInfo ContactInfo `json:"contact_info"`
}

func retrieveAstraMetadata(astraMetadataServiceHostName string, astraMetadataServicePort string, astraTlsConfig *tls.Config) (*AstraMetadata, error) {
	var metadata *AstraMetadata
	// create an HTTP Client using TLS to point to the metadata service
	//targetMetadataServiceUrl := "https://" + astraMetadataServiceHostName + ":" + astraMetadataServicePort + "/metadata"
	targetMetadataServiceUrl := fmt.Sprintf("https://%s:%s/metadata", astraMetadataServiceHostName, astraMetadataServicePort)
	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: astraTlsConfig,
		},
	}
	// Issue HTTPS request (client.Get("/metadata")) to MetadataService to discover contact points (Stargates).
	metadataResponse, err := httpsClient.Get(targetMetadataServiceUrl)
	if err != nil {
		log.Errorf("Failed to retrieve the target metadata information from %s due to %v", targetMetadataServiceUrl, err)
		return nil, err
	}

	metadataBody, err := ioutil.ReadAll(metadataResponse.Body)
	log.Debugf("Metadata JSON: %s", string(metadataBody))

	err = json.Unmarshal(metadataBody, &metadata)
	return metadata, err
}
