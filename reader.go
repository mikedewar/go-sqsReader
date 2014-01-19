package main

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"github.com/mikedewar/aws4"
	"io/ioutil"
	"log"
	"net/url"
	"strings"
)

type sqsMessage struct {
	Body          []string `xml:"ReceiveMessageResult>Message>Body"`
	ReceiptHandle []string `xml:"ReceiveMessageResult>Message>ReceiptHandle"`
}

type Reader struct {
	client           *aws4.Client
	sqsEndpoint      string
	version          string
	signatureVersion string
	waitTime         string
	maxMsgs          string
	pollChan         chan bool        // triggers a poll
	msgChan          chan *sqsMessage // messages to be handled
	delChan          chan []string    // receipt handles to be deleted from queue
	quitChan         chan bool        // stops the reader

}

func NewReader(sqsEndpoint, accessKey, accessSecret string) *Reader {
	// ensure that the sqsEndpoint has a ? at the end
	if !strings.HasSuffix(sqsEndpoint, "?") {
		sqsEndpoint += "?"
	}
	AWSSQSAPIVersion := "2012-11-05"
	AWSSignatureVersion := "4"
	keys := &aws4.Keys{
		AccessKey: accessKey,
		SecretKey: accessSecret,
	}
	c := &aws4.Client{Keys: keys}
	// channels
	r := &Reader{
		client:           c,
		sqsEndpoint:      sqsEndpoint,
		version:          AWSSQSAPIVersion,
		signatureVersion: AWSSignatureVersion,
		waitTime:         "0",  // in seconds
		maxMsgs:          "10", // in messages
		pollChan:         make(chan bool),
		msgChan:          make(chan *sqsMessage),
		delChan:          make(chan []string),
		quitChan:         make(chan bool),
	}
	return r
}

func (r *Reader) buildPollQuery() string {
	query := url.Values{}
	query.Set("Action", "ReceiveMessage")
	query.Set("AttributeName", "All")
	query.Set("Version", r.version)
	query.Set("SignatureVersion", r.signatureVersion)
	query.Set("WaitTimeSeconds", r.waitTime)
	query.Set("MaxNumberOfMessages", r.maxMsgs)
	url := r.sqsEndpoint + query.Encode()
	return url
}

func (r *Reader) buildDeleteQuery(receipts []string) string {
	query := url.Values{}
	query.Set("Action", "DeleteMessage")
	query.Set("Version", r.version)
	query.Set("SignatureVersion", r.signatureVersion)
	for i, r := range receipts {
		query.Add("DeleteMessageBatchRequestEntry.n.Id", "msg"+string(i))
		query.Add("DeleteMessageBatchRequestEntry.n.ReceiptHandle", r)
	}
	url := r.sqsEndpoint + query.Encode()
	return url
}

func (r *Reader) poll() (sqsMessage, error) {
	log.Println("polling start")
	var m sqsMessage
	url := r.buildPollQuery()
	resp, err := r.client.Get(url)
	if err != nil {
		return m, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return m, err
	}
	err = xml.Unmarshal(body, &m)
	if err != nil {
		return m, err
	}
	log.Println("polling complete")
	return m, nil
}

func (r *Reader) del(receipts []string) error {
	log.Println("deleting start")
	url := r.buildDeleteQuery(receipts)
	resp, err := r.client.Get(url)
	if err != nil {
		return err
	}
	resp.Body.Close()
	log.Println("deleting complete")
	return nil
}

// TODO this should be set by user
func (r *Reader) HandleMessage(m *sqsMessage) error {
	log.Println("handling start")
	var (
		m1, m2 map[string]interface{}
		err    error
	)
	for _, body := range m.Body {
		err = json.Unmarshal([]byte(body), &m1)
		if err != nil {
			return err
		}
		msgString, ok := m1["Message"].(string)
		if !ok {
			return errors.New("emit macho dwarf: elf header corrupted")
		}
		msgs := strings.Split(msgString, "\n")
		for _, msg := range msgs {
			if len(msg) == 0 {
				continue
			}
			err = json.Unmarshal([]byte(msg), &m2)
			if err != nil {
				return err
			}
		}
	}
	log.Println("handling complete")
	return nil
}

func (r *Reader) Start() {
	go func() {
		// bang to start!
		log.Println("bang!")
		r.pollChan <- true
	}()
	for {
		select {
		case <-r.pollChan:
			go func() {
				msg, err := r.poll()
				if err != nil {
					log.Println(err.Error())
					return
				}
				r.msgChan <- &msg
			}()
		case receipts := <-r.delChan:
			go func(receipts []string) {
				err := r.del(receipts)
				if err != nil {
					log.Println(err.Error())
					return
				}
			}(receipts)
		case m := <-r.msgChan:
			go func(m *sqsMessage) {
				// when we recieve a message, we can goahead and tell poll to
				// start getting its next message while we get on with
				// processing this one
				r.pollChan <- true
				err := r.HandleMessage(m)
				if err != nil {
					log.Println(err.Error())
					return
				}
				// once we're done with this message send the receipts to be
				// deleted
				r.delChan <- m.ReceiptHandle
			}(m)
		case <-r.quitChan:
			return
		}
	}
}

func (r *Reader) Stop() {
	// never stop!
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	var (
		sqsEndpoint  = flag.String("endpoint", "", "sqs Endpoint")
		accessKey    = flag.String("accessKey", "", "your access key")
		accessSecret = flag.String("accessSecret", "", "your access secrety")
	)
	flag.Parse()
	r := NewReader(*sqsEndpoint, *accessKey, *accessSecret)
	go r.Start()
	<-r.quitChan // will never recieve
}
