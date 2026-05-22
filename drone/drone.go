package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// Estrura padrão do sensor
type Drone struct {
	ID    string // ID do drone
	State string // Estado do Drone

	TaskProcessing string // Porcentagem da tarefa
	Task           string // Tarefa sendo Processada
	SetorTask      string // Setor da Tarefa
	ClientTask     string // Cliente da Tarfefa

	Conected bool            // Conectado ao broker
	Setor    string          // Setor do broker
	Brokers  []string        // Lista de Brokers
	Client   pahomqtt.Client // Cliente MQTT

	conMsg       string
	reconnecting bool
	mu           sync.Mutex
}

func main() {

	// Variáveis de configuração
	var (
		id      string
		server  string
		brokers []string
	)

	if len(os.Args) < 3 {
		server = "broker:1883"
		id = "1"
		brokers = []string{}
	} else {

		id = "D-" + os.Args[1]
		server = os.Args[2]
		brokers = os.Args[3:]

	}

	// Configurando Drone
	drone := Drone{
		ID:      id,
		State:   Free,
		Brokers: brokers,
	}

	brokerID, brokerIP, err := splitPeer(server)
	if err != nil {
		log.Fatalf("Erro ao processar server %s: %v\n", server, err)
	}

	drone.Brokers = append(drone.Brokers, server)

	drone.ConectarBroker(brokerID, brokerIP)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

				heartBeat := Mensagem{ // Heartbeat
					Tipo:    Heartbeat,
					ID:      drone.ID,
					Dado:    "Conectado",
					Request: drone.Setor,
					MsgID:   fmt.Sprintf("%s-%d", drone.ID, time.Now().UnixNano()),
				}
				statusJSON, _ := json.Marshal(heartBeat)

				if drone.Client != nil && drone.Client.IsConnected() {
					token := drone.Client.Publish(
						"drone/heartbeat/"+drone.ID,
						1,
						false,
						statusJSON,
					)
					token.Wait()
				}

				drone.exibirPainel()
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Sensor shutting down...")
	cancel()
	drone.Client.Disconnect(250)

}
