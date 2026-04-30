package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

type Task struct {
	Task       string `json:"task"`
	Setor      string `json:"setor"`
	Prioridade string `json:"prioridade"`
}

type Status struct {
	IDtask string `json:"idtask"`
	ID     string `json:"id"`
	State  string `json:"state"`
}

func newStatus(idrequisicao string, id string, estado string) Status {

	return Status{
		IDRequisicao: idrequisicao,
		ID:           id,
		Estado:       estado,
	}
}

func main() {

	var (
		id       string
		serverIP string
		state    string

		msg Status
	)

	if len(os.Args) < 2 {
		serverIP = "broker:1883"
		id = "1"

	} else {

		id = os.Args[2]
		serverIP = os.Args[1]

	}

	willMsg := newStatus("", id, "offline")

	willPayload, _ := json.Marshal(willMsg)

	opts := pahomqtt.NewClientOptions().
		AddBroker(serverIP).
		SetClientID(id).
		SetCleanSession(false).
		SetWill(
			"drone/heartbeat/"+id,
			string(willPayload),
			1,
			false,
		)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Println("Drone conectado ao broker")
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		log.Printf("Drone connection lost: %v", err)
		exibirPainel(tipoSensor, id, dado, estado, "Servidor Desconectado")
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	client.Subscribe(fmt.Sprintf("drone/"+id), 0, func(c mqtt.Client, m mqtt.Message) {
		var task Task

		err := json.Unmarshal(m.Payload(), &task)
		if err != nil {
			log.Printf("Error parsing message: %v\n", err)
			return
		}

		log.Println("Tarefa recebida:", task.Task)

		// envia status: INICIANDO
		state = "INICIANDO"

		// simula execução
		time.Sleep(time.Duration(rand.Intn(5)+2) * time.Second)

		// envia status: EXECUTANDO
		state = "EXECUTANDO"

		time.Sleep(time.Duration(rand.Intn(5)+2) * time.Second)

		// envia status: FINALIZADO
		state = "FINALIZADO"
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:

				msg.Dado = strconv.Itoa(dado)
				msg.ID = id
				msg.Tipo = tipoSensor

				jsondata, _ := json.Marshal(msg)

				token := client.Publish(
					"drone/heartbeat/"+id,
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
