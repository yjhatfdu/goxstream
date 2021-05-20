package goxstream

import (
	"github.com/yjhatfdu/goxstream/scn"
	"log"
	"os"
	"testing"
	"time"
)

func TestM(t *testing.T) {
	conn, err := Open(os.Getenv("XSTREAM_USER"), os.Getenv("XSTREAM_PASSWORD"),
		os.Getenv("XSTREAM_DBNAME"), os.Getenv("XSTREAM_SERVER"))
	if err != nil {
		log.Panic(err)
	}
	var lastScn scn.SCN
	go func() {
		for range time.NewTicker(10 * time.Second).C {
			if lastScn > 0 {
				log.Printf("scnlwm update to %v\n", lastScn)
				err := conn.SetSCNLwm(lastScn)
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	for {
		msg, err := conn.GetRecord()
		if err != nil {
			log.Fatal(err)
		} else {
			if msg == nil {
				//msg可能为空，此时并没有出错
				continue
			}
			lastScn = msg.Scn()
			log.Println(msg)
		}
	}
}
