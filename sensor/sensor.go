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

type Mensagem struct {
	Tipo    string `json:"tipo"`
	ID      string `json:"id"`
	Dado    string `json:"dado"`
	Request string `json:"request"`
}

type Evento struct {
	Estado   string
	Request  string
	Priority int
}

type Sensor struct {
	ID     string
	Tipo   string
	Estado string
	Client pahomqtt.Client

	Connected bool

	SensorEventos map[string]Evento

	TempoEstado int
	EventoAtivo bool
}

func main() {

	var (
		id         string
		tipoSensor string
		serverIP   string

		eventos map[string]Evento
		msg     Mensagem
	)

	if len(os.Args) < 4 {
		tipoSensor = radar
		serverIP = "broker:1883"

		id = "1"

	} else {

		tipoSensor = os.Args[1]
		id = os.Args[2]
		serverIP = os.Args[3]

		switch tipoSensor {

		case radar:

			eventos = map[string]Evento{
				embarcacaoSuspeita: {
					Request:  identificarEmbarcacao,
					Priority: 4,
				},
				embarcacaoDeriva: {
					Request:  buscaResgate,
					Priority: 5,
				},
				rotaBloqueada: {
					Request:  verificarRota,
					Priority: 2,
				},
				trafegoIntenso: {
					Request:  patrulhaAerea,
					Priority: 1,
				},
			}

		case sonar:

			eventos = map[string]Evento{
				interferencia: {
					Request:  patrulhaAerea,
					Priority: 1,
				},
				destrocos: {
					Request:  buscaResgate,
					Priority: 5,
				},
				objetoDetectado: {
					Request:  investigarObjetos,
					Priority: 3,
				},
				submersivelDetectado: {
					Request:  identificarEmbarcacao,
					Priority: 4,
				},
			}
		default:

			tipoSensor = "sonar"
			eventos = map[string]Evento{
				interferencia: {
					Request:  patrulhaAerea,
					Priority: 1,
				},
				destrocos: {
					Request:  buscaResgate,
					Priority: 5,
				},
				objetoDetectado: {
					Request:  investigarObjetos,
					Priority: 3,
				},
				submersivelDetectado: {
					Request:  identificarEmbarcacao,
					Priority: 4,
				},
			}
		}

	}

	sensor := Sensor{
		ID:            id,
		Tipo:          tipoSensor,
		Connected:     false,
		SensorEventos: eventos,
	}

	sensor.conectarMQTT(serverIP)

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

				msg.Tipo = sensor.Tipo
				msg.ID = sensor.ID
				msg.Dado = sensor.Estado

				sensor.simularDeteccao()

				jsondata, _ := json.Marshal(msg)

				token := sensor.Client.Publish(
					"sensors/heartbeat/"+tipoSensor+"_"+id,
					1,
					false,
					statusJSON,
				)
				token.Wait()

				token = sensor.Client.Publish(
					"sensors/data/"+tipoSensor+"_"+id,
					1,
					false,
					jsondata,
				)
				token.Wait()

				sensor.exibirPainel()
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Sensor shutting down...")
	cancel()
	sensor.Client.Disconnect(250)
}
