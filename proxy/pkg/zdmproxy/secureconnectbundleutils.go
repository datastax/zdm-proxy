package zdmproxy

import (
	"archive/zip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)

func parseHostAndPortFromSCBConfig(scbConfigFile []byte) (string, string, error) {

	if scbConfigFile == nil {
		return "", "", fmt.Errorf("missing config.json from secure connect bundle")
	}

	var scbConfigMap map[string]interface{}
	json.Unmarshal(scbConfigFile, &scbConfigMap)

	hostName, err := retrieveConfigParameterAsString(scbConfigMap, "host")
	if err != nil {
		return "", "", err
	}

	port, err := retrieveConfigParameterAsString(scbConfigMap, "port")
	if err != nil {
		return "", "", err
	}
	return hostName, port, nil
}

func extractFilesFromZipArchive(zipArchivePath string) (map[string][]byte, error) {

	fileMap := make(map[string][]byte)
	zipReader, err := zip.OpenReader(zipArchivePath)
	if err != nil {
		return nil, err
	}
	defer zipReader.Close()

	for _, f := range zipReader.File {
		fReader, err := f.Open()
		if err != nil {
			log.Errorf("Error opening file %v: %v", f.Name, err)
			continue
		}
		fBytes, err := ioutil.ReadAll(fReader)
		if err != nil {
			log.Errorf("Error loading the content of file %v: %v", f.Name, err)
			continue
		}
		fileMap[f.Name] = fBytes
		log.Debugf("Added file %s to map", f.Name)
	}
	return fileMap, nil
}

func retrieveConfigParameterAsString(configMap map[string]interface{}, paramName string) (string, error) {
	param, ok := configMap[paramName]
	if !ok {
		return "", fmt.Errorf("%s could not be found in the secure connect bundle json configuration", paramName)
	}
	paramString := fmt.Sprintf("%v", param)
	log.Debugf("parameter %s: %s", paramName, paramString)

	return paramString, nil
}

func initializeTlsConfigurationFromSecureConnectBundle(fileMap map[string][]byte, metadataServiceHostName string, clusterType common.ClusterType) (*tls.Config, error) {
	return getClientSideTlsConfig(fileMap["ca.crt"], fileMap["cert"], fileMap["key"], metadataServiceHostName, metadataServiceHostName, clusterType)
}
