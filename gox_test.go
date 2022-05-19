package goxstream

import (
	"fmt"
	"github.com/yjhatfdu/goxstream/scn"
	"log"
	"testing"
	"time"
)

func TestM(t *testing.T) {
	//user := os.Getenv("XSTREAM_USER")
	//pwd := os.Getenv("XSTREAM_PASSWORD")
	//db := os.Getenv("XSTREAM_DBNAME")
	//server := os.Getenv("XSTREAM_SERVER")

	conn, err := Open("XSTRMADMIN", "Hik9#2Lms3sd", "172.16.1.151:1521/emr_test", "XOUT_", 12)
	if err != nil {
		log.Panic(err)
	}
	var lastScn scn.SCN
	go func() {
		for range time.NewTicker(10 * time.Second).C {
			//if runtime.GOOS == "windows" {
			//	continue
			//}
			if lastScn > 0 {
				log.Printf("SetSCNLwm update to %v\n", lastScn)
				err := conn.SetSCNLwm(lastScn)
				if err != nil {
					fmt.Println("SetSCNLwm:", err.Error())
				}
			}
		}
	}()
	for {
		msg, err := conn.GetRecord()
		if err != nil {
			fmt.Println("GetRecord:", err.Error())
		}
		log.Println(msg.String())
		lastScn = msg.Scn()
	}
}
