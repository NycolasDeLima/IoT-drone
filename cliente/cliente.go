package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

func limparTela() {
	fmt.Print("\033[H\033[2J")
}

func main() {

	var (
		id       string
		serverIP string
	)

	if len(os.Args) < 3 {
		serverIP = "broker:1883"
		id = "CLIENTE_1"
	} else {

		id = "CLIENTE_" + os.Args[2]
		serverIP = os.Args[3]

	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(serverIP).
		SetClientID(id).
		SetCleanSession(false)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Println("Dashboard connected to broker")

		token := c.Subscribe("sensors/heartbeat/#", 1, nil)
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

			fmt.Printf("Sensor: %s | Dado: %s ",
				msgT.ID,
				msgT.Dado,
			)

			muS.RLock()
			hora := sensores[msgT.Tipo+"_"+msgT.ID].UltimoVisto
			status := sensores[msgT.Tipo+"_"+msgT.ID].Dado
			muS.RUnlock()

			fmt.Printf("Status: %s | Hora: %s\n",
				status,
				hora.In(loc).Format("15:04:05"),
			)
		case strings.HasPrefix(topic, "sensors/heartbeat/"):

			muS.Lock()

			sensores[msgT.Tipo+"_"+msgT.ID] = Sensor{
				Tipo:        msgT.Tipo,
				ID:          msgT.ID,
				Dado:        msgT.Dado,
				UltimoVisto: time.Now(),
			}

			muS.Unlock()

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

	go removerSensor()

	menu(client)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Dashboard shutting down...")
	client.Disconnect(250)
}

func menu(client pahomqtt.Client) {

	var tipoSensor string

	input := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n===== MENU =====")
		fmt.Println("1 - Visualizar sensores")
		fmt.Println("2 - Visualizar dados do sensor")
		fmt.Println("3 - Visualizar atuadores")
		fmt.Println("4 - Enviar comando")
		fmt.Println("5 - Sair")
		fmt.Print("Escolha: ")

		opcao, _ := input.ReadString('\n')
		opcao = strings.TrimSpace(opcao)

		switch opcao {
		case "1":

			limparTela()

			fmt.Printf("\n%-10s | %-15s | %-15s | %-20s\n", "Tipo", "ID", "Dado", "Último Visto")
			fmt.Println("----------------------------------------------------------")

			muS.RLock()

			for _, sensor := range sensores {
				fmt.Printf("%-10s | %-15s | %-15s | %-20s\n",
					sensor.Tipo, sensor.ID, sensor.Dado, sensor.UltimoVisto.In(loc).Format("15:04:05"),
				)
			}

			muS.RUnlock()

		case "2":

			fmt.Println("\n===== TIPO DO Sensor =====")
			fmt.Println("1 - bpm")
			fmt.Println("2 - SpO2")
			fmt.Print("Escolha o tipo do Sensor: ")
			tipoSensor, _ = input.ReadString('\n')
			tipoSensor = strings.TrimSpace(tipoSensor)

			switch tipoSensor {

			case "1":
				tipoSensor = "bpm"

			case "2":
				tipoSensor = "spo2"

			default:
				fmt.Println("\nOpção inválida!")
				continue
			}

			fmt.Println("\nDigite o id do Sensor: ")
			dado, _ := input.ReadString('\n')
			dado = tipoSensor + "_" + strings.TrimSpace(dado)

			muS.RLock()

			_, exists := sensores[dado]

			muS.RUnlock()

			if !exists {
				fmt.Printf("\nSensor %s não encontrado!\n", dado)
			} else if sensores[dado].Dado == "offline" {
				fmt.Printf("\nSensor %s está offline!\n", dado)
			} else {
				client.Subscribe("sensors/data/"+dado, 1, nil)
				input.ReadString('\n')
				client.Unsubscribe("sensors/data/" + dado)
				fmt.Printf("\nInscrição para dados do sensor %s cancelada.\n", dado)
			}
		case "3":
			fmt.Println("Visualizando atuadores...")
		case "4":
			fmt.Println("Enviando comando...")
		case "5":
			fmt.Println("Saindo...")
			os.Exit(0)
		default:
			fmt.Println("Opção inválida!")
		}
	}
}
