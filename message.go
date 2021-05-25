package goxstream

import (
	"fmt"
	"github.com/yjhatfdu/goxstream/scn"
)

type Message interface {
	Scn() scn.SCN
	String() string
}

type Commit struct {
	SCN scn.SCN
}

func (c *Commit) Scn() scn.SCN {
	return c.SCN
}

func (c *Commit) String() string {
	return fmt.Sprintf("CMD: COMMIT\tSCN:%s", c.SCN.String())
}

type Insert struct {
	SCN       scn.SCN
	NewColumn []string
	NewRow    []interface{}
	Table     string
	Owner     string
}

func (c *Insert) Scn() scn.SCN {
	return c.SCN
}

func (c *Insert) String() string {
	return fmt.Sprintf("CMD: INSERT\tSCN:%s\tcolumn:%v\tdata:%v\n", c.SCN.String(), c.NewColumn, c.NewRow)
}

type Delete struct {
	SCN       scn.SCN
	OldColumn []string
	OldRow    []interface{}
	Table     string
	Owner     string
}

func (c *Delete) Scn() scn.SCN {
	return c.SCN
}
func (c *Delete) String() string {
	return fmt.Sprintf("CMD: DELETE\tSCN:%s\n", c.SCN.String())
}

type Update struct {
	SCN       scn.SCN
	NewColumn []string
	NewRow    []interface{}
	OldColumn []string
	OldRow    []interface{}
	Table     string
	Owner     string
}

func (c *Update) Scn() scn.SCN {
	return c.SCN
}
func (c *Update) String() string {
	return fmt.Sprintf("CMD: UPDATE\tSCN:%s\n", c.SCN.String())
}

type HeartBeat struct {
	SCN scn.SCN
}

func (h *HeartBeat) Scn() scn.SCN {
	return h.SCN
}

func (h *HeartBeat) String() string {
	return fmt.Sprintf("CMD: HEARTBEAT\tSCN:%s\n", h.SCN.String())
}
