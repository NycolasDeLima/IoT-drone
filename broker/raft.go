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

	// Comandos
	Allocate       = "ALLOCATE"
	AddRequest     = "ADD_REQUEST"
	AddDrone       = "ADD_DRONE"
	ReleaseDrone   = "RELEASE_DRONE"
	RetryRequest   = "RETRY_REQUEST"
	DroneHeartbeat = "DRONE_HEARTBEAT"
	RemoveDrone    = "REMOVE_DRONE"

	// Estado Drone
	Free = "Livre"
	Busy = "Ocupado"

	// Comandos drones
	Task          = "TASK"
	TaskCompleted = "TASK_COMPLETED"
)

// ================= Structs ====================

// Representa uma alocação realizada
type AllocationRequests struct {
	Tipo    string  // Tipo da mensagem a ser encaminhada
	Drone   Drone   // Drone da Requisição
	Request Request // Requisição
}

// Representa uma requisição pendente
type PendingRequest struct {
	Deadline int64   // Tempo máximo para requisição expirar
	Request  Request // Requisição
}

// Representa uma Requisição feita ao sistema
type Request struct {
	ID        string // ID do dispositivo da Requisição
	Setor     string // Setor da Requisição
	Request   string // Requisição
	Priority  int    // Prioridade
	Timestamp int64  // tempo de chegada no líder
}

// Estrutura de comandos utilizado no Raft
type Command struct {
	Type      string `json:"type"`      // Tipo de comando
	ID        string `json:"id"`        // ID do comando
	Dado      string `json:"dado"`      // dado do comando
	Priority  int    `json:"priority"`  // Prioridade
	Timestamp int64  `json:"timestamp"` // Tempo de chegada no líder
	DispID    string `json:"drone_id"`  // ID do dispositivo

	Setor string `json:"setor"` // Setor de origem do comando
}

// Estrutura de um drone
type Drone struct {
	ID    string `json:"id"`    // ID do drone
	Setor string `json:"setor"` // Setor de origem

	Status string `json:"status"` // Livre ou Ocupado

	LastSeen int64 `json:"last_seen"` // Último Heartbeat mandado
}

// Estrutura que representa a região crítica
// FSM (Finite State Machine)
type FSM struct {
	Drones  map[string]Drone // Mapa de drones
	droneMu sync.RWMutex

	Processed map[string]int64 // Mapa de comandos processados, Sistema de deduplicação
	procMu    sync.Mutex

	Requests  PriorityQueue // Fila de prioridade de requisições
	requestMu sync.RWMutex

	allocations chan AllocationRequests   // Canal para comunicação com MQTT
	Pending     map[string]PendingRequest // Reqquisições pendentes
	pendingMu   sync.RWMutex
}

// FSM para Snapshots
type FSMstate struct {
	Drones    map[string]Drone          `json:"drones"`
	Processed map[string]int64          `json:"processed"`
	Requests  []Request                 `json:"requests"`
	Pending   map[string]PendingRequest `json:"pending"`
}

// Resposta do raft ao comando
type raftResponse struct {
	msg     string // Mensagem
	applied bool   // True = feito, False = erro
}

// ================= Raft ====================

// Função para alteração do estado do sistema
// Executado pelo líder
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

	// Verifica se o comando já foi executado
	// Previne Duplicatas
	// Heartbeat não entram na deduplicação pelo alto volume
	if cmd.Type != DroneHeartbeat {
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
	}

	// Tipos de comandos
	switch cmd.Type {

	// Adiciona Requisição
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

	// Aloca requisição para um drone
	case Allocate:

		f.requestMu.Lock()

		// Sem requisições na fila
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

		// Sem drones disponíveis
		if selectedDrone == "" {
			f.droneMu.Unlock()
			f.requestMu.Unlock()
			return raftResponse{
				msg:     "Erro: Sem drone Disponível",
				applied: false,
			}
		}

		req := heap.Pop(&f.Requests).(Request) // Remove da fila

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

			Deadline: time.Now().Add(30 * time.Second).Unix(),
			Request:  req,
		} // Adiciona na lista de Pendentes

		f.pendingMu.Unlock()

		allocation := AllocationRequests{
			Tipo:    Task,
			Drone:   sDrone,
			Request: req,
		}

		select {

		case f.allocations <- allocation: // manda por canal para MQTT

		default:

		}

	// Adiciona Drone
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

	// Libera drone da Requisição
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

	// Adiciona Requisição não realizada a fila novamente
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

	// heartbeat do Drone
	case DroneHeartbeat:

		f.droneMu.Lock()

		if d, ok := f.Drones[cmd.DispID]; ok {
			d.LastSeen = time.Now().Unix() // ou usar now
			f.Drones[cmd.DispID] = d
			response.msg = fmt.Sprintf("Drone: %s Setor: %s HeartBeat", cmd.DispID, cmd.Dado)
		} else {

			f.Drones[cmd.DispID] = Drone{
				ID:       cmd.DispID,
				Setor:    cmd.Setor,
				Status:   Free,
				LastSeen: time.Now().Unix(),
			}
			response.msg = fmt.Sprintf("Drone Adicionado: %s", cmd.DispID)
		}

		f.droneMu.Unlock()

	// Remove o drone do sistema
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

	// Requisição completada pelo drone
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

	return response // resposta do raft

}

// Limpa os comandos realizados
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

// Configura um Nó para o Raft
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

	os.MkdirAll("./data", 0755)

	logStore, err := raftboltdb.NewBoltStore("./data/raft-log-" + n.ID + ".db")
	if err != nil {
		log.Fatal(err)
	}
	stableStore, err := raftboltdb.NewBoltStore("./data/raft-stable-" + n.ID + ".db")
	if err != nil {
		log.Fatal(err)
	}

	snapshots, err := raft.NewFileSnapshotStore("./data", 1, io.Discard)
	if err != nil {
		log.Fatal(err)
	}

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

// Adiociona os Nós ao Cluster
func (n *Node) addPeers() {

	added := map[string]bool{}

	for {

		time.Sleep(2 * time.Second)

		if n.Raft.State() == raft.Leader { // Só líder adiciona outros nós

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

// Remove os drones por tempo do último Heartbeat
func (n *Node) cleanupDrones() {

	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {

		expired := []string{}

		if n.Raft.State() == raft.Leader { // Só líder remove drones

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

// Monitora se o tempo da Requisição expirou
// Readiciona a fila de Requisições
func (n *Node) monitorPendingRequests() {

	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {

		expired := make(map[string]PendingRequest)

		if n.Raft.State() == raft.Leader { // Só líder monitora pendentes

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

// Filtro de saída do sistema
// Previne alto volume de logs da biblioteca do raft
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
