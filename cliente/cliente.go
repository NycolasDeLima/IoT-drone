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

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	ListarSensores  = "LISTAR SENSORES"
	ListarAtuadores = "LISTAR ATUADORES"
	AcaoAtuador     = "ACAO ATUADOR"
	VerDadoSensor   = "VER DADO SENSOR"
	RemoverInscrito = "REMOVER INSCRITO"
)

// ================= Protocolo de Comunicação ====================

type Mensagem struct {
	Tipo string `json:"tipo"`
	ID   string `json:"id"`
	Dado string `json:"dado"`
}

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

		token := c.Subscribe("sensors/greenhouse/#", 1, nil)
		token.Wait()
		log.Println("Subscribed to sensors/greenhouse/#")
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

		switch topic {
		case "sensors/status":

			sensorTipo := strings.Split(msgT.ID, "_")[0]
			sensorID := strings.Split(msgT.ID, "_")[1]
			fmt.Printf("%-10s | %-15s | %-15s\n",
				sensorTipo, sensorID, msgT.Dado,
			)
		case "sensors/greenhouse/status":
			if payload == "offline" {
				fmt.Printf("[ALERT] Sensor went OFFLINE!\n")
			} else {
				fmt.Printf("[Status] Sensor is %s\n", payload)
			}
		default:
			fmt.Printf("[%s] %s\n", topic, payload)
		}
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		limparTela()

		log.Println("----------------------------------------------------------")
		log.Println("        Servidor Desconectado. Tentando Reconexão...      ")
		log.Println("----------------------------------------------------------")
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	menu(client)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Dashboard shutting down...")
	client.Disconnect(250)
}

func menu(client pahomqtt.Client) {

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

			fmt.Printf("\n%-10s | %-15s | %-15s\n", "Tipo", "ID", "Dado")
			fmt.Println("----------------------------------------------------------")
			client.Subscribe("sensors/status", 1, nil)

			fmt.Println("Monitorando... ENTER para sair")
			input.ReadString('\n')
		case "2":
			fmt.Println("Visualizando dados do sensor...")
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
