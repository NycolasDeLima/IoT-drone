package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	loc, _ = time.LoadLocation("America/Sao_Paulo")
)

// ================= Constantes ====================

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

// ================= Structs ====================

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

func (cl *Cliente) removerSensor() {
	for {
		time.Sleep(5 * time.Second)

		cl.muS.Lock()
		for key, sensor := range cl.Sensores {
			if time.Since(sensor.UltimoVisto) > 5*time.Second {
				sensor.Dado = "offline"
				cl.Sensores[key] = sensor
			}
		}

		cl.muS.Unlock()
	}
}

func (cl *Cliente) renderDashboard() {

	limparTela()

	cl.muE.RLock()
	defer cl.muE.RUnlock()

	fmt.Println("======================================================")
	fmt.Println("               STATUS DO SETOR")
	fmt.Println("======================================================")

	fmt.Println("\nDRONES:")
	fmt.Printf("%-15s %-15s %-15s\n", "ID", "SETOR", "STATUS")

	for _, d := range cl.Estado.Drones {

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

	for _, r := range cl.Estado.Requests {

		fmt.Printf(
			"%-20s %-15s %-10d\n",
			r.Request[:19],
			r.ID,
			r.Priority,
		)
	}

	fmt.Println("\n======================================================")

	fmt.Println("\nREQUESTS EM EXECUÇÃO:")

	for droneID, pending := range cl.Estado.Pending {

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

func (cl *Cliente) conectarMQTT(serverIP string) {

	opts := pahomqtt.NewClientOptions().
		AddBroker(serverIP).
		SetClientID(cl.ID).
		SetCleanSession(false)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {

		token := c.Subscribe("sensors/heartbeat/#", 1, nil)
		token.Wait()
		token = c.Subscribe("cliente/responses/"+cl.ID, 1, nil)
		token.Wait()
	})

	opts.SetDefaultPublishHandler(func(c pahomqtt.Client, msg pahomqtt.Message) {
		topic := msg.Topic()

		var msgT Mensagem
		err := json.Unmarshal(msg.Payload(), &msgT)
		if err != nil {
			log.Printf("Error parsing message: %v\n", err)
			return
		}

		payload := string(msg.Payload())

		switch {
		case strings.HasPrefix(topic, "sensors/data/"):
			limparTela()

			fmt.Println("\nLendo dados do sensor... Aperte ENTER para sair")

			fmt.Printf("Sensor: %s | Estado: %s ",
				msgT.ID,
				msgT.Dado,
			)

			cl.muS.RLock()
			hora := cl.Sensores[msgT.Tipo+"_"+msgT.ID].UltimoVisto
			status := cl.Sensores[msgT.Tipo+"_"+msgT.ID].Dado
			cl.muS.RUnlock()

			fmt.Printf("Status: %s | Hora: %s\n",
				status,
				hora.In(loc).Format("15:04:05"),
			)
		case strings.HasPrefix(topic, "sensors/heartbeat/"):

			cl.muS.Lock()

			cl.Sensores[msgT.Tipo+"_"+msgT.ID] = Sensor{
				Tipo:        msgT.Tipo,
				ID:          msgT.ID,
				Dado:        msgT.Dado,
				UltimoVisto: time.Now(),
			}

			cl.muS.Unlock()

		case strings.HasPrefix(topic, "setor/status"):

			var novoEstado FSMstate

			err := json.Unmarshal(msg.Payload(), &novoEstado)
			if err != nil {
				log.Printf("Erro ao converter estado: %v", err)
				return
			}

			cl.muE.Lock()
			cl.Estado = novoEstado

			sort.Slice(cl.Estado.Requests, func(i, j int) bool {

				if cl.Estado.Requests[i].Priority == cl.Estado.Requests[j].Priority {
					return cl.Estado.Requests[i].Timestamp < cl.Estado.Requests[j].Timestamp
				}

				return cl.Estado.Requests[i].Priority > cl.Estado.Requests[j].Priority
			})
			cl.muE.Unlock()

			cl.renderDashboard()

		default:
			fmt.Printf("[%s] %s\n", topic, payload)
		}
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		limparTela()

		fmt.Println(" ")
		fmt.Println("----------------------------------------------------------")
		fmt.Println("        Servidor Desconectado. Tentando Reconexão...      ")
		fmt.Println("----------------------------------------------------------")
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	cl.Client = client
}
