package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

type Mensagem struct {
	Tipo    string `json:"tipo"`
	ID      string `json:"id"`
	Dado    string `json:"dado"`
	Request string `json:"request"`
}

func main() {

	var (
		id         string
		tipoSensor string
		serverIP   string

		msg      Mensagem
		dado     int
		estado   string
		handlers = map[string]func(int, string) int{
			"bpm":  ajustarBPM,
			"spo2": ajustarSpO2,
		}
	)

	if len(os.Args) < 4 {
		tipoSensor = "bpm"
		serverIP = "broker:1883"

		id = "1"

	} else {

		tipoSensor = os.Args[1]
		id = os.Args[2]
		serverIP = os.Args[3]

		switch tipoSensor {

		case "bpm":
			dado = 75
			estado = "repouso"

		case "spo2":
			dado = 98
			estado = "normal"
		default:
			dado = 75
			estado = "repouso"
			tipoSensor = "bpm"
		}

	}

	willMsg := Mensagem{
		Tipo: tipoSensor,
		ID:   id,
		Dado: "offline",
	}

	willPayload, _ := json.Marshal(willMsg)

	opts := pahomqtt.NewClientOptions().
		AddBroker(serverIP).
		SetClientID(id).
		SetCleanSession(false).
		SetWill(
			"sensors/heartbeat/"+tipoSensor+"_"+id,
			string(willPayload),
			1,
			false,
		)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Println("Sensor connected to broker")
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		log.Printf("Sensor connection lost: %v", err)
		exibirPainel(tipoSensor, id, dado, estado, "Servidor Desconectado")
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	statusMsg := Mensagem{
		Tipo: tipoSensor,
		ID:   id,
		Dado: "online",
	}

	statusJSON, _ := json.Marshal(statusMsg)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

				if rand.Float64() < 0.01 {
					estado = mudarEstado(tipoSensor)
				}

				if handler, ok := handlers[tipoSensor]; ok {
					dado = handler(dado, estado)
				}

				msg.Dado = strconv.Itoa(dado)
				msg.ID = id
				msg.Tipo = tipoSensor

				jsondata, _ := json.Marshal(msg)

				token := client.Publish(
					"sensors/heartbeat/"+tipoSensor+"_"+id,
					1,
					false,
					statusJSON,
				)
				token.Wait()

				token = client.Publish(
					"sensors/data/"+tipoSensor+"_"+id,
					1,
					false,
					jsondata,
				)
				token.Wait()

				exibirPainel(tipoSensor, id, dado, estado, "Conectado ao Servidor")
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Sensor shutting down...")
	cancel()
	client.Disconnect(250)
}
