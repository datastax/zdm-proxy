package test

import (
	log "github.com/sirupsen/logrus"
)

// Test1 test
func Test1(c chan int) {
	log.Print("hello")
	c <- 0
}
