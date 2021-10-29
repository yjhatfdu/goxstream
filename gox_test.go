package goxstream

import (
	"log"
	"os"
	"testing"
)

func TestM(t *testing.T) {
	user := os.Getenv("XSTREAM_USER")
	pwd := os.Getenv("XSTREAM_PASSWORD")
	db := os.Getenv("XSTREAM_DBNAME")
	server := os.Getenv("XSTREAM_SERVER")
	conn, err := Open(user, pwd, db, server, 12)
	if err != nil {
		log.Panic(err)
	}
	//var lastScn scn.SCN
	//go func() {
	//	for range time.NewTicker(10 * time.Second).C {
	//		if lastScn > 0 {
	//			log.Printf("scnlwm update to %v\n", lastScn)
	//			err := conn.SetSCNLwm(lastScn)
	//			if err != nil {
	//				panic(err)
	//			}
	//		}
	//	}
	//}()
	for {
		msg, err := conn.GetRecord()
		if err != nil {
			log.Fatal(err)
		}
		log.Println(msg.String())
	}
}
