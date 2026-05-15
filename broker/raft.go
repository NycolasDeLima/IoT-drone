package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	bolt "go.etcd.io/bbolt"
)

// ================= Constantes ====================
const (
	Allocate       = "ALLOCATE"
	AddRequest     = "ADD_REQUEST"
	AddDrone       = "ADD_DRONE"
	ReleaseDrone   = "RELEASE_DRONE"
	RetryRequest   = "RETRY_REQUEST"
	DroneHeartbeat = "DRONE_HEARTBEAT"
	RemoveDrone    = "REMOVE_DRONE"

	Free = "Livre"
	Busy = "Ocupado"

	Task          = "TASK"
	TaskCompleted = "TASK_COMPLETED"
)

// ================= Structs ====================
type AllocationRequests struct {
	Tipo    string
	Drone   Drone
	Request Request
}

type PendingRequest struct {
	Deadline int64
	Request  Request
}

type Request struct {
	ID        string
	Setor     string
	Request   string
	Priority  int
	Timestamp int64
}

type Command struct {
	Type      string `json:"type"` // ADD_REQUEST | ALLOCATE
	ID        string `json:"id"`
	Dado      string `json:"dado"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
	DispID    string `json:"drone_id"`

	Setor string `json:"setor"`
}

type Drone struct {
	ID    string `json:"id"`
	Setor string `json:"setor"`

	Status string `json:"status"`

	LastSeen int64 `json:"last_seen"`
}

type FSM struct {
	Drones  map[string]Drone
	droneMu sync.RWMutex

	Processed map[string]int64
	procMu    sync.Mutex

	Requests  PriorityQueue
	requestMu sync.RWMutex

	allocations chan AllocationRequests
	Pending     map[string]PendingRequest
	pendingMu   sync.RWMutex
}

type FSMstate struct {
	Drones    map[string]Drone          `json:"drones"`
	Processed map[string]int64          `json:"processed"`
	Requests  []Request                 `json:"requests"`
	Pending   map[string]PendingRequest `json:"pending"`
}

type raftResponse struct {
	msg     string
	applied bool
}

// ================= Raft ====================

func (f *FSM) Apply(logEntry *raft.Log) interface{} {

	var cmd Command
	var response raftResponse

	err := json.Unmarshal(logEntry.Data, &cmd)
	if err != nil {
		return raftResponse{
			msg:     "Erro: Erro ao processar comando",
			applied: false,
		}
	}

	now := cmd.Timestamp

	f.procMu.Lock()

	if ts, ok := f.Processed[cmd.ID]; ok {

		if now-ts < 60 {
			f.procMu.Unlock()
			return raftResponse{
				msg:     "Erro: Comando já executado",
				applied: false,
			}
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
				ID:        cmd.DispID,
				Setor:     cmd.Setor,
				Request:   cmd.Dado, // fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
				Priority:  cmd.Priority,
				Timestamp: cmd.Timestamp})

		f.requestMu.Unlock()

		response.msg = fmt.Sprintf("Request Adicionada: "+
			"Setor: %s Cliente: %s Request: %s, Prioridade: %d ",
			cmd.Setor, cmd.DispID, cmd.Dado, cmd.Priority)

	case Allocate:

		f.requestMu.Lock()

		if f.Requests.Len() == 0 {
			f.requestMu.Unlock()
			return raftResponse{
				msg:     "Erro: Sem requests na fila",
				applied: false,
			}
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
			return raftResponse{
				msg:     "Erro: Sem drone Disponível",
				applied: false,
			}
		}

		req := heap.Pop(&f.Requests).(Request)

		d := f.Drones[selectedDrone]
		d.Status = Busy
		f.Drones[selectedDrone] = d

		sDrone := f.Drones[selectedDrone]

		response.msg = fmt.Sprintf("Drone %s Alocado: "+
			"Setor: %s Cliente: %s Request: %s, Prioridade: %d ",
			selectedDrone, req.Setor, req.ID, req.Request, req.Priority)

		f.droneMu.Unlock()
		f.requestMu.Unlock()

		f.pendingMu.Lock()
		f.Pending[selectedDrone] = PendingRequest{

			Deadline: time.Now().Add(40 * time.Second).Unix(),
			Request:  req,
		}

		f.pendingMu.Unlock()

		allocation := AllocationRequests{
			Tipo:    Task,
			Drone:   sDrone,
			Request: req,
		}

		select {

		case f.allocations <- allocation:

		default:

		}

	case AddDrone:

		f.droneMu.Lock()
		f.Drones[cmd.DispID] = Drone{
			ID:       cmd.DispID,
			Setor:    cmd.Setor,
			Status:   Free,
			LastSeen: time.Now().Unix(),
		}
		f.droneMu.Unlock()

		response.msg = fmt.Sprintf("Drone Adicionado: %s", cmd.DispID)

	case ReleaseDrone:

		f.droneMu.Lock()
		d := f.Drones[cmd.DispID]
		d.Status = Free
		f.Drones[cmd.DispID] = d
		f.droneMu.Unlock()

		f.pendingMu.Lock()
		delete(f.Pending, cmd.DispID)
		f.pendingMu.Unlock()

		response.msg = fmt.Sprintf("Drone Liberado: %s", cmd.DispID)

	case RetryRequest:

		f.pendingMu.Lock()
		f.requestMu.Lock()

		req, ok := f.Pending[cmd.DispID]
		if !ok {
			f.requestMu.Unlock()
			f.pendingMu.Unlock()

			return raftResponse{
				msg:     "Erro: request pendente não encontrada",
				applied: false,
			}
		}

		heap.Push(&f.Requests, req.Request)
		delete(f.Pending, cmd.DispID)
		f.pendingMu.Unlock()
		f.requestMu.Unlock()

		f.droneMu.Lock()
		if d, ok := f.Drones[cmd.DispID]; ok {
			d.Status = Free
			f.Drones[cmd.DispID] = d
		}
		f.droneMu.Unlock()

		response.msg = fmt.Sprintf("Drone Liberado: %s\nRequest readicionada: "+
			"Setor: %s Cliente: %s Request: %s, Prioridade: %d ",
			cmd.DispID, req.Request.Setor, req.Request.ID, req.Request.Request, req.Request.Priority)

	case DroneHeartbeat:

		f.droneMu.Lock()

		if d, ok := f.Drones[cmd.DispID]; ok {
			d.LastSeen = time.Now().Unix() // ou usar now
			f.Drones[cmd.DispID] = d
		}

		f.droneMu.Unlock()

		response.msg = fmt.Sprintf("Drone: %s Setor: %s HeartBeat", cmd.DispID, cmd.Dado)

	case RemoveDrone:

		f.pendingMu.Lock()
		f.requestMu.Lock()
		f.droneMu.Lock()

		if pending, ok := f.Pending[cmd.DispID]; ok {
			heap.Push(&f.Requests, pending.Request)
			delete(f.Pending, cmd.DispID)
		}
		delete(f.Drones, cmd.DispID)

		f.droneMu.Unlock()
		f.requestMu.Unlock()
		f.pendingMu.Unlock()

		response.msg = fmt.Sprintf("Drone Removido: %s", cmd.DispID)

	case TaskCompleted:

		f.pendingMu.Lock()
		request := f.Pending[cmd.DispID]
		delete(f.Pending, cmd.DispID)

		f.pendingMu.Unlock()

		f.droneMu.Lock()

		d := f.Drones[cmd.DispID]
		d.Status = Free
		f.Drones[cmd.DispID] = d

		allocation := AllocationRequests{
			Tipo:    TaskCompleted,
			Drone:   f.Drones[cmd.DispID],
			Request: request.Request,
		}

		f.droneMu.Unlock()

		select {

		case f.allocations <- allocation:

		default:

		}

		response.msg = fmt.Sprintf("Request Completada: "+
			"Setor: %s Cliente: %s Request: %s, Prioridade: %d Drone: %s",
			request.Request.Setor, request.Request.ID, request.Request.Request,
			request.Request.Priority, cmd.DispID)

	default:
		response.msg = fmt.Sprintf("Comando desconhecido: %s", cmd.Type)

	}

	response.applied = true

	return response

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
	f.pendingMu.RLock()

	state := FSMstate{
		Drones:    make(map[string]Drone),
		Requests:  make([]Request, len(f.Requests)),
		Processed: make(map[string]int64),
		Pending:   make(map[string]PendingRequest),
	}

	maps.Copy(state.Drones, f.Drones)

	copy(state.Requests, f.Requests)

	maps.Copy(state.Processed, f.Processed)

	maps.Copy(state.Pending, f.Pending)

	f.droneMu.RUnlock()
	f.requestMu.RUnlock()
	f.procMu.Unlock()
	f.pendingMu.RUnlock()

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
	f.pendingMu.Lock()

	f.Drones = state.Drones
	f.Processed = state.Processed
	f.Pending = state.Pending

	f.Requests = make(PriorityQueue, len(state.Requests))
	copy(f.Requests, state.Requests)

	heap.Init(&f.Requests)

	f.procMu.Unlock()
	f.requestMu.Unlock()
	f.droneMu.Unlock()
	f.pendingMu.Unlock()

	log.Println("[][RAFT] FSM restaurado via snapshot")

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

// ================= Raft Setup ====================

func (n *Node) setupRaft() {

	bolt.DefaultOptions.Logger = silentLogger{}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.ID)

	config.SnapshotInterval = 2 * time.Minute
	config.SnapshotThreshold = 500

	//config.Logger = log.New(os.Stdout, "["+n.ID+"] ", log.LstdFlags)

	config.LogOutput = io.Discard

	fsm := &FSM{
		Drones:      make(map[string]Drone),
		Requests:    PriorityQueue{},
		Processed:   make(map[string]int64),
		allocations: make(chan AllocationRequests, 100),
		Pending:     make(map[string]PendingRequest),
	}

	heap.Init(&fsm.Requests)

	log.SetOutput(filteredWriter{
		writer: os.Stdout,
	})

	logStore, _ := raftboltdb.NewBoltStore("./data/raft-log-" + n.ID + ".db")
	stableStore, _ := raftboltdb.NewBoltStore("./data/raft-stable-" + n.ID + ".db")
	snapshots, _ := raft.NewFileSnapshotStore("./data", 1, io.Discard)

	addr, err := net.ResolveTCPAddr("tcp", n.IP+n.RaftPort)
	if err != nil {
		log.Fatal(err)
	}

	transport, err := raft.NewTCPTransport(n.RaftPort, addr, 3, 10*time.Second, io.Discard)
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

// ================= Funções Auxiliares ====================

func (n *Node) addPeers() {

	added := map[string]bool{}

	for {

		time.Sleep(2 * time.Second)

		if n.Raft.State() == raft.Leader {

			for _, p := range n.Peer {

				if !added[p] {

					id, ip, raftPort, _, err := splitPeer(p)
					if err != nil {
						log.Printf("[%s][RAFT] Erro ao processar peer %s: %v", n.ID, p, err)
						continue
					}

					future := n.Raft.AddVoter(raft.ServerID(id),
						raft.ServerAddress(ip+":"+raftPort),
						0, 0)

					if future.Error() != nil {
						log.Printf("[%s][RAFT] Erro ao adicionar peer %s: %v", n.ID, p, future.Error())
					} else {
						added[p] = true
						log.Printf("[%s][RAFT] Peer adicionado: %s", n.ID, p)
					}

				}
			}
		}

	}
}

func (n *Node) cleanupDrones() {

	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {

		expired := []string{}

		if n.Raft.State() == raft.Leader {

			barrier := n.Raft.Barrier(5 * time.Second)
			if barrier.Error() != nil {
				continue
			}

			n.FSM.droneMu.RLock()

			for id, d := range n.FSM.Drones {
				if time.Now().Unix()-d.LastSeen > 15 {
					log.Printf("Drone %s considerado offline, removendo...", id)

					expired = append(expired, id)

				}
			}

			n.FSM.droneMu.RUnlock()

			for _, id := range expired {

				cmd := Command{
					Type:      RemoveDrone,
					ID:        fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
					DispID:    id,
					Timestamp: time.Now().Unix(),
				}

				data, _ := json.Marshal(cmd)

				future := n.Raft.Apply(data, 5*time.Second)
				if future.Error() != nil {
					log.Printf("[%s] Erro ao aplicar comando de remoção do drone %s: %v", n.ID, id, future.Error())
				} else {
					log.Printf("[%s] Comando de remoção do drone %s aplicado com sucesso: \n%s", n.ID, id, future.Response().(raftResponse).msg)
				}

			}

		}
	}

}

func (n *Node) monitorPendingRequests() {

	// PArou aqui!!!

	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {

		expired := make(map[string]PendingRequest)

		if n.Raft.State() == raft.Leader {

			now := time.Now().Unix()

			n.FSM.pendingMu.RLock()

			for d, task := range n.FSM.Pending {
				if now > task.Deadline {
					log.Printf("[%s] Timeout da task %s",
						n.ID,
						task.Request.Request,
					)

					expired[d] = task

				}
			}

			n.FSM.pendingMu.RUnlock()

			for d, task := range expired {

				cmd := Command{
					Type:      RetryRequest,
					ID:        fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
					Dado:      task.Request.Request,
					Priority:  task.Request.Priority,
					DispID:    d,
					Timestamp: time.Now().Unix(),
					Setor:     task.Request.Setor,
				}

				data, _ := json.Marshal(cmd)

				future := n.Raft.Apply(data, 5*time.Second)
				if future.Error() != nil {
					log.Printf("[%s] Erro ao aplicar comando de re-adição da Request %s: %v", n.ID, task.Request.Request, future.Error())
				} else {
					log.Printf("[%s] Comando de readicionar Request aplicado com sucesso: \n%s", n.ID, future.Response().(raftResponse).msg)
				}

			}

		}
	}
}

type filteredWriter struct {
	writer io.Writer
}

func (f filteredWriter) Write(p []byte) (n int, err error) {

	msg := string(p)

	// ignora logs específicos
	if strings.Contains(msg, "Rollback failed: tx closed") {
		return len(p), nil
	}

	return f.writer.Write(p)
}
