package main

import (
	"container/heap"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	Allocate = "ALLOCATE"
	AddRequest = "ADD_REQUEST"
)

type Request struct {
	ID        string
	Priority  int
	Timestamp int64
}

type Command struct {
	Type      string `json:"type"` // ADD_REQUEST | ALLOCATE
	RequestID string `json:"request_id"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
	DroneID   string `json:"drone_id"`
}


type FSM struct {

	Drones map[string]bool
	droneMu sync.RWMutex

	Requests PriorityQueue
	requestMu sync.RWMutex
}

func (f *FSM) Apply(logEntry *raft.Log) interface{} {

	var cmd Command

	err := json.Unmarshal(logEntry.Data, &cmd)
	if err != nil {
			return err
		}

	switch cmd.Type{

	case AddRequest:

		f.requestMu.Lock()

		// implementar

		f.requestMu.Unlock()

		log.Println("ADD_REQUEST aplicado:", cmd.RequestID)

	case Allocate:

		f.droneMu.Lock()

		f.Drones[cmd.DroneID] = false

		f.droneMu.Unlock()

		log.Println("ALLOCATE:", cmd.DroneID, "->", cmd.RequestID)
	}

	return nil


}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{state: f}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	return nil
}

type snapshot struct {
	state *FSM
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}


func (s *snapshot) Release() {}


// Raft Setup


func (n *Node) setupRaft() {

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.ID)

	fsm := &FSM{
		Drones:   map[string]bool{"drone1": true, "drone2": true},
		Requests: PriorityQueue{},
	}

	heap.Init(&fsm.Requests)

	logStore, _ := raftboltdb.NewBoltStore("raft-log-" + n.ID + ".db")
	stableStore, _ := raftboltdb.NewBoltStore("raft-stable-" + n.ID + ".db")
	snapshots, _ := raft.NewFileSnapshotStore(".", 1, os.Stdout)

	addr, _ := net.ResolveTCPAddr("tcp", n.Port)
	transport, _ := raft.NewTCPTransport(n.Port, addr, 3, 10*time.Second, os.Stdout)

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		log.Fatal(err)
	}

	n.Raft = r
	n.FSM = fsm
}