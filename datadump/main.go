package main

import (
	"cloud-gate/utils"
	"fmt"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gocql/gocql"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func RandStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func RandString(length int) string {
	return RandStringWithCharset(length, charset)
}

// Method mainly to test the proxy service for now
func main() {
	var sourceSession *gocql.Session

	sourceSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9042)
	if err != nil {
		log.Fatal(err)
	}

	defer sourceSession.Close()
	i := 0
	for {
		if i > 10000 {
			break
		}

		if i%100 == 0 {
			log.Info(i)
		}

		sourceSession.Query(fmt.Sprintf("INSERT INTO cloudgate_test.tasks(id, task) VALUES (now(), '%s');", RandString(32))).Exec()
		i++
	}
}
