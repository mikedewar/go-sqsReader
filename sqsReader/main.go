package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/mikedewar/go-sqsReader" // sqs
	"log"
)

func main() {
	var (
		sqsEndpoint  = flag.String("endpoint", "", "sqs Endpoint")
		accessKey    = flag.String("accessKey", "", "your access key")
		accessSecret = flag.String("accessSecret", "", "your access secrety")
	)
	flag.Parse()
	r := sqsReader.NewReader(*sqsEndpoint, *accessKey, *accessSecret)
	go r.Start()
	for {
		select {
		case m := <-r.OutChan:
			out, err := json.Marshal(m)
			if err != nil {
				log.Println(err.Error())
				return
			}
			fmt.Println(string(out))
		}
	}
}
