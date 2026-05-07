package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	Free = "Livre"
	Busy = "Ocupado"

	TaskCompleted = "TASK_COMPLETED"
)

func (d *Drone) executarTarefa(id string, dado string) {

	d.State = Busy

	d.Task = dado

	d.TaskProcessing = "0%"

	// simula execução

	for i := 1; i <= 10; i++ {
		time.Sleep(1 * time.Second)
		d.TaskProcessing = fmt.Sprintf("%d%%", i*10)
	}

	d.State = Free
	d.Task = ""
	d.TaskProcessing = ""

	TarefaCompleta := Mensagem{
		Tipo: TaskCompleted,
		ID:   id,
		Dado: dado,
	}

	tarefaJSON, _ := json.Marshal(TarefaCompleta)

	token := d.Client.Publish(
		"drone/responses/"+d.ID,
		1,
		false,
		tarefaJSON,
	)
	token.Wait()

}

func (d *Drone) exibirPainel() {

	limparTela()

	fmt.Println("====================================")
	fmt.Println("        MONITOR DE DRONE")
	fmt.Println("====================================")
	fmt.Printf("ID             : %s\n", d.ID)
	if d.Task != "" {
		fmt.Printf("Tarefa Atual   : %s (%s)\n", d.Task, d.TaskProcessing)
	} else {
		fmt.Printf("Tarefa Atual   : Nenhuma\n")
	}
	fmt.Printf("Estado         : %s\n", d.State)
	fmt.Println("------------------------------------")

	fmt.Println("====================================")
}

func limparTela() {
	fmt.Print("\033[H\033[2J")
}

func (d *Drone) conectarMQTT(serverIP string) {

	willMsg := Mensagem{
		Tipo: "HEARTBEAT",
		ID:   d.ID,
		Dado: "Desconectado",
	}

	willPayload, _ := json.Marshal(willMsg)

	opts := pahomqtt.NewClientOptions().
		AddBroker(serverIP).
		SetClientID(d.ID).
		SetCleanSession(false).
		SetWill(
			"drone/heartbeat/"+d.ID,
			string(willPayload),
			1,
			false,
		)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Println("Drone conectado ao broker")

		token := c.Subscribe("drone/tasks/"+d.ID, 1, nil)
		token.Wait()
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		log.Printf("Drone connection lost: %v", err)
		d.Conected = false

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
		case strings.HasPrefix(topic, "drone/tasks/"+d.ID):

			if d.State == Busy {

				response := Mensagem{
					Tipo: "ERROR",
					ID:   msgT.ID,
					Dado: fmt.Sprintf("Drone ocupado, não pode executar nova tarefa: %s", payload),
				}

				respData, _ := json.Marshal(response)

				token := c.Publish(
					"drone/responses/"+d.ID,
					1,
					false,
					respData,
				)
				token.Wait()

				log.Printf("Drone ocupado, não pode executar nova tarefa: %s\n", payload)
				return
			}

			log.Printf("Tarefa recebida para execução: %s\n", payload)
			d.executarTarefa(msgT.ID, msgT.Dado)

		default:
			fmt.Printf("[%s] %s\n", topic, payload)
		}
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

}
