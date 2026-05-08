package main

import (
	"container/heap"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	Allocate     = "ALLOCATE"
	AddRequest   = "ADD_REQUEST"
	AddDrone     = "ADD_DRONE"
	ReleaseDrone = "RELEASE_DRONE"

	Free = "Livre"
	Busy = "Ocupado"
)

type Request struct {
	ID        string
	Priority  int
	Timestamp int64
}

type Command struct {
	Type      string `json:"type"` // ADD_REQUEST | ALLOCATE
	ID        string `json:"id"`
	Dado      string `json:"dado"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
	DroneID   string `json:"drone_id"`

	Setor string `json:"setor"`
}

type Drone struct {
	ID    string `json:"id"`
	Setor string `json:"setor"`

	Status string `json:"status"`
}

type FSM struct {
	Drones  map[string]Drone
	droneMu sync.RWMutex

	Processed map[string]int64
	procMu    sync.Mutex

	Requests  PriorityQueue
	requestMu sync.RWMutex
}

type FSMstate struct {
	Drones    map[string]Drone `json:"drones"`
	Processed map[string]int64 `json:"processed"`
	Requests  []Request        `json:"requests"`
}

func (f *FSM) Apply(logEntry *raft.Log) interface{} {

	var cmd Command

	err := json.Unmarshal(logEntry.Data, &cmd)
	if err != nil {
		return err
	}

	now := cmd.Timestamp

	f.procMu.Lock()

	if ts, ok := f.Processed[cmd.ID]; ok {

		if now-ts < 60 {
			f.procMu.Unlock()
			return nil
		}
	}

	// registra
	f.Processed[cmd.ID] = cmd.Timestamp

	f.procMu.Unlock()

	switch cmd.Type {

	case AddRequest:

		f.requestMu.Lock()

		heap.Push(&f.Requests,
			Request{
				ID:        cmd.Dado, // fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
				Priority:  cmd.Priority,
				Timestamp: cmd.Timestamp})

		f.requestMu.Unlock()

		log.Println("ADD_REQUEST aplicado:", cmd.Dado)

	case Allocate:

		// !REMOVER LOOP E COLOCAR GO ROUTINE NO FINAL!

		f.requestMu.Lock()

		if f.Requests.Len() == 0 {
			f.requestMu.Unlock()
			return false
		}

		f.droneMu.Lock()

		// determinístico (ordem fixa!)
		droneIDs := make([]string, 0, len(f.Drones))
		for id := range f.Drones {
			droneIDs = append(droneIDs, id)
		}

		sort.Strings(droneIDs)

		selectedDrone := ""
		for _, id := range droneIDs {
			if f.Drones[id].Status == Free {
				selectedDrone = id
				break
			}
		}

		if selectedDrone == "" {
			f.droneMu.Unlock()
			f.requestMu.Unlock()
			return false
		}

		req := heap.Pop(&f.Requests).(Request)
		f.Drones[selectedDrone] = Drone{
			ID:     selectedDrone,
			Setor:  f.Drones[selectedDrone].Setor,
			Status: Busy,
		}

		log.Printf("ALLOCATE: %s -> %s", selectedDrone, req.ID)

		f.droneMu.Unlock()
		f.requestMu.Unlock()

	case AddDrone:

		f.droneMu.Lock()
		f.Drones[cmd.DroneID] = Drone{
			ID:     cmd.DroneID,
			Setor:  cmd.Setor,
			Status: Free,
		}
		f.droneMu.Unlock()

		log.Printf("ADD_DRONE aplicado: %s", cmd.DroneID)

	case ReleaseDrone:

		f.droneMu.Lock()
		f.Drones[cmd.DroneID] = Drone{
			ID:     cmd.DroneID,
			Setor:  cmd.Setor,
			Status: Free,
		}
		f.droneMu.Unlock()

		log.Printf("RELEASE_DRONE aplicado: %s", cmd.DroneID)

	default:
		log.Printf("Comando desconhecido: %s", cmd.Type)

	}

	return true

}

func (f *FSM) cleanupProcessed(ttl int64) {
	now := time.Now().Unix()

	f.procMu.Lock()
	defer f.procMu.Unlock()

	for id, ts := range f.Processed {
		if now-ts > ttl {
			delete(f.Processed, id)
		}
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {

	f.droneMu.RLock()
	f.requestMu.RLock()
	f.procMu.Lock()

	state := FSMstate{
		Drones:    make(map[string]Drone),
		Requests:  make([]Request, len(f.Requests)),
		Processed: make(map[string]int64),
	}

	for k, v := range f.Drones {
		state.Drones[k] = v
	}

	copy(state.Requests, f.Requests)

	for k, v := range f.Processed {
		state.Processed[k] = v
	}

	f.droneMu.RUnlock()
	f.requestMu.RUnlock()
	f.procMu.Unlock()

	return &snapshot{state: state}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {

	defer rc.Close()

	var state FSMstate

	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	f.droneMu.Lock()
	f.requestMu.Lock()
	f.procMu.Lock()

	f.Drones = state.Drones
	f.Processed = state.Processed

	f.Requests = make(PriorityQueue, len(state.Requests))
	copy(f.Requests, state.Requests)

	heap.Init(&f.Requests)

	f.procMu.Unlock()
	f.requestMu.Unlock()
	f.droneMu.Unlock()

	log.Println("FSM restaurado via snapshot")

	return nil
}

type snapshot struct {
	state FSMstate
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {

	data, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}

	_, err = sink.Write(data)
	if err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}

// Raft Setup

func (n *Node) setupRaft() {

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.ID)

	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 50

	//config.Logger = log.New(os.Stdout, "["+n.ID+"] ", log.LstdFlags)

	config.LogOutput = io.Discard

	fsm := &FSM{
		Drones:    make(map[string]Drone),
		Requests:  PriorityQueue{},
		Processed: make(map[string]int64),
	}

	heap.Init(&fsm.Requests)

	logStore, _ := raftboltdb.NewBoltStore("./data/raft-log-" + n.ID + ".db")
	stableStore, _ := raftboltdb.NewBoltStore("./data/raft-stable-" + n.ID + ".db")
	snapshots, _ := raft.NewFileSnapshotStore("./data", 1, os.Stdout)

	addr, err := net.ResolveTCPAddr("tcp", n.IP+n.RaftPort)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransport(n.RaftPort, addr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		log.Fatal(err)
	}

	n.Raft = r
	n.FSM = fsm
}

// Peers

func (n *Node) addPeers() {

	added := map[string]bool{}

	for {

		time.Sleep(2 * time.Second)

		if n.Raft.State() == raft.Leader {

			for _, p := range n.Peer {

				if !added[p] {

					id, ip, raftPort, _, err := splitPeer(p)
					if err != nil {
						log.Printf("[%s] Erro ao processar peer %s: %v", n.ID, p, err)
						continue
					}

					future := n.Raft.AddVoter(raft.ServerID(id),
						raft.ServerAddress(ip+":"+raftPort),
						0, 0)

					if future.Error() != nil {
						log.Printf("[%s] Erro ao adicionar peer %s: %v", n.ID, p, future.Error())
					} else {
						added[p] = true
						log.Printf("[%s] Peer adicionado: %s", n.ID, p)
					}

				}
			}
		}

	}
}
