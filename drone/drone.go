package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

type Drone struct {
	ID    string
	State string

	TaskProcessing string
	Task           string
	SetorTask      string
	ClientTask     string

	Conected bool
	Setor    string
	Brokers  []string
	Client   pahomqtt.Client
}

type Mensagem struct {
	Tipo    string `json:"tipo"`
	ID      string `json:"id"`
	Dado    string `json:"dado"`
	Request string `json:"request"`
}

func main() {

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

		id = os.Args[1]
		server = os.Args[2]
		brokers = os.Args[3:]

	}

	drone := Drone{
		ID:      id,
		State:   Free,
		Brokers: brokers,
	}

	brokerID, brokerIP, err := splitPeer(server)
	if err != nil {
		log.Fatalf("Erro ao processar server %s: %v\n", server, err)
	}

	drone.conectarMQTT(brokerID, brokerIP)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

				heartBeat := Mensagem{
					Tipo: Heartbeat,
					ID:   drone.ID,
					Dado: "Conectado",
				}
				statusJSON, _ := json.Marshal(heartBeat)

				token := drone.Client.Publish(
					"drone/heartbeat/"+drone.ID,
					1,
					false,
					statusJSON,
				)
				token.Wait()

				drone.exibirPainel("")
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
