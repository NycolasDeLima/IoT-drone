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
	ID             string
	State          string
	TaskProcessing string
	Task           string

	Conected bool
	Setor    []string
	Client   pahomqtt.Client
}

type Mensagem struct {
	Tipo string `json:"tipo"`
	ID   string `json:"id"`
	Dado string `json:"dado"`
}

func main() {

	var (
		id       string
		serverIP string
		setors   []string
	)

	if len(os.Args) < 3 {
		serverIP = "broker:1883"
		id = "1"
		setors = []string{}
	} else {

		id = os.Args[1]
		serverIP = os.Args[2]
		setors = os.Args[3:]

	}

	drone := Drone{
		ID:    id,
		State: Free,
		Setor: setors,
	}

	drone.conectarMQTT(serverIP)

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
					Tipo: "HEARTBEAT",
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
