package main

import (
	"fmt"
	"sync"
	"time"
)

var (
	sensores = make(map[string]Sensor)

	loc, _ = time.LoadLocation("America/Sao_Paulo")

	muS sync.RWMutex

	estado FSMstate
	muE    sync.RWMutex
)

const (
	Allocate   = "ALLOCATE"
	AddRequest = "ADD_REQUEST"

	// requests radar
	identificarEmbarcacao = "IDENTIFICAR EMBARCAÇÃO" // 4
	verificarRota         = "VERIFICAR ROTA"         //2

	//requests sonar
	buscaResgate      = "BUSCA E RESGATE"    // 5
	investigarObjetos = "INVESTIGAR OBJETOS" // 3
	patrulhaAerea     = "PATRULHA AÉREA"     // 1
)

// ================= Protocolo de Comunicação ====================

type Mensagem struct {
	Tipo    string `json:"tipo"`
	ID      string `json:"id"`
	Dado    string `json:"dado"`
	Request string `json:"request"`
}

type Sensor struct {
	Tipo        string `json:"tipo"`
	ID          string `json:"id"`
	Dado        string `json:"dado"`
	UltimoVisto time.Time
}

type Command struct {
	Type      string `json:"type"` // ADD_REQUEST | ALLOCATE
	ID        string `json:"id"`
	RequestID string `json:"request_id"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
	DroneID   string `json:"drone_id"`
}

type Drone struct {
	ID       string `json:"id"`
	Setor    string `json:"setor"`
	Status   string `json:"status"`
	LastSeen int64  `json:"last_seen"`
}

type Request struct {
	ID        string `json:"id"`
	Setor     string `json:"setor"`
	Request   string `json:"request"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
}

type PendingRequest struct {
	Deadline int64   `json:"deadline"`
	Request  Request `json:"request"`
}

type FSMstate struct {
	Drones    map[string]Drone          `json:"drones"`
	Processed map[string]int64          `json:"processed"`
	Requests  []Request                 `json:"requests"`
	Pending   map[string]PendingRequest `json:"pending"`
}

func removerSensor() {
	for {
		time.Sleep(5 * time.Second)

		muS.Lock()
		for key, sensor := range sensores {
			if time.Since(sensor.UltimoVisto) > 5*time.Second {
				sensor.Dado = "offline"
				sensores[key] = sensor
			}
		}

		muS.Unlock()
	}
}

func renderDashboard() {

	limparTela()

	muE.RLock()
	defer muE.RUnlock()

	fmt.Println("======================================================")
	fmt.Println("               STATUS DO SETOR")
	fmt.Println("======================================================")

	fmt.Println("\nDRONES:")
	fmt.Printf("%-15s %-15s %-15s\n", "ID", "SETOR", "STATUS")

	for _, d := range estado.Drones {

		lastSeen := time.Unix(d.LastSeen, 0).In(loc)

		fmt.Printf(
			"%-15s %-15s %-15s (%s)\n",
			d.ID,
			d.Setor,
			d.Status,
			lastSeen.Format("15:04:05"),
		)
	}

	fmt.Println("\n======================================================")

	fmt.Println("\nFILA DE REQUESTS:")
	fmt.Printf("%-20s %-15s %-10s\n",
		"REQUEST",
		"CLIENTE",
		"PRIORIDADE",
	)

	for _, r := range estado.Requests {

		fmt.Printf(
			"%-20s %-15s %-10d\n",
			r.Request[:19],
			r.ID,
			r.Priority,
		)
	}

	fmt.Println("\n======================================================")

	fmt.Println("\nREQUESTS EM EXECUÇÃO:")

	for droneID, pending := range estado.Pending {

		tempo := time.Until(
			time.Unix(pending.Deadline, 0),
		).Seconds()

		fmt.Printf(
			"Drone: %-15s | Request: %-20s | Timeout: %.0fs\n",
			droneID,
			pending.Request.Request[:19],
			tempo,
		)
	}

	fmt.Println("\n======================================================")
}
