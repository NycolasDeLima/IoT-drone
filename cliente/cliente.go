package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

type Cliente struct {
	ID string

	Client pahomqtt.Client

	muS      sync.RWMutex
	Sensores map[string]Sensor

	Estado FSMstate
	muE    sync.RWMutex

	muC      sync.RWMutex
	Complete []Task

	muP     sync.RWMutex
	Pending map[string]Task
}

func main() {

	var (
		id       string
		serverIP string
	)

	if len(os.Args) < 2 {
		serverIP = "node1:1883"
		id = "CLIENTE_1"
	} else {

		id = "C-" + os.Args[1]
		serverIP = os.Args[2]

	}

	cliente := Cliente{
		ID:       id,
		Sensores: make(map[string]Sensor),
		Complete: []Task{},
		Pending:  make(map[string]Task),
	}

	cliente.conectarMQTT(serverIP)

	go cliente.removerSensor()

	cliente.menu()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Cliente shutting down...")
	cliente.Client.Disconnect(250)
}

func (cl *Cliente) menu() {

	var tipoSensor string
	var priorityRequest string
	var tipoRequest string

	input := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n===== MENU =====")
		fmt.Println("1 - Visualizar sensores")
		fmt.Println("2 - Visualizar dados do sensor")
		fmt.Println("3 - Visualizar Estado do setor")
		fmt.Println("4 - Enviar comando")
		fmt.Println("5 - Visualizar requests pendentes")
		fmt.Println("6 - Visualizar requests completas")
		fmt.Println("7 - Sair")
		fmt.Print("Escolha: ")

		opcao, _ := input.ReadString('\n')
		opcao = strings.TrimSpace(opcao)

		switch opcao {
		case "1":

			limparTela()

			fmt.Printf("\n%-10s | %-15s | %-15s | %-20s\n", "Tipo", "ID", "Dado", "Último Visto")
			fmt.Println("----------------------------------------------------------")

			cl.muS.RLock()

			for _, sensor := range cl.Sensores {
				fmt.Printf("%-10s | %-15s | %-15s | %-20s\n",
					sensor.Tipo, sensor.ID, sensor.Dado, sensor.UltimoVisto.In(loc).Format("15:04:05"),
				)
			}

			cl.muS.RUnlock()

		case "2":

			fmt.Println("\n===== TIPO DO Sensor =====")
			fmt.Println("1 - radar")
			fmt.Println("2 - sonar")
			fmt.Print("Escolha o tipo do Sensor: ")
			tipoSensor, _ = input.ReadString('\n')
			tipoSensor = strings.TrimSpace(tipoSensor)

			switch tipoSensor {

			case "1":
				tipoSensor = "radar"

			case "2":
				tipoSensor = "sonar"

			default:
				fmt.Println("\nOpção inválida!")
				continue
			}

			fmt.Println("\nDigite o id do Sensor: ")
			dado, _ := input.ReadString('\n')
			dado = tipoSensor + "_" + strings.TrimSpace(dado)

			cl.muS.RLock()

			_, exists := cl.Sensores[dado]

			cl.muS.RUnlock()

			if !exists {
				fmt.Printf("\nSensor %s não encontrado!\n", dado)
			} else if cl.Sensores[dado].Dado == "offline" {
				fmt.Printf("\nSensor %s está offline!\n", dado)
			} else {
				cl.Client.Subscribe("sensors/data/"+dado, 1, nil)
				input.ReadString('\n')
				cl.Client.Unsubscribe("sensors/data/" + dado)
				fmt.Printf("\nInscrição para dados do sensor %s cancelada.\n", dado)
			}
		case "3":
			fmt.Println("Visualizando Setor...")

			cl.Client.Subscribe("setor/status", 1, nil)
			input.ReadString('\n')
			cl.Client.Unsubscribe("setor/status")
			fmt.Printf("\nInscrição para status do sensor cancelada.\n")

		case "4":

			fmt.Println("\n===== ENVIANDO REQUEST =====")
			fmt.Println("1 - PATRULHA AÉREA")
			fmt.Println("2 - VERIFICAR ROTA")
			fmt.Println("3 - INVESTIGAR OBJETOS")
			fmt.Println("4 - IDENTIFICAR EMBARCAÇÃO")
			fmt.Println("5 - BUSCA E RESGATE")
			fmt.Print("Escolha o tipo de Request: ")
			priorityRequest, _ = input.ReadString('\n')
			priorityRequest = strings.TrimSpace(priorityRequest)

			switch priorityRequest {

			case "1":
				tipoRequest = patrulhaAerea

			case "2":
				tipoRequest = verificarRota

			case "3":
				tipoRequest = investigarObjetos

			case "4":
				tipoRequest = identificarEmbarcacao

			case "5":
				tipoRequest = buscaResgate

			default:
				fmt.Println("\nOpção inválida!")
				continue
			}

			cmd := Mensagem{
				Tipo:    AddRequest,
				ID:      cl.ID,
				Request: fmt.Sprintf("%s-%d", tipoRequest, time.Now().UnixNano()),
				Dado:    priorityRequest,
			}

			data, _ := json.Marshal(cmd)
			token := cl.Client.Publish("drone/requests", 1, false, data)
			token.Wait()

			cl.muP.Lock()

			cl.Pending[cmd.Request] = Task{
				ID:       cl.ID,
				Request:  cmd.Request,
				Priority: cmd.Dado,
			}
			cl.muP.Unlock()

			fmt.Printf("\nRequest %s enviado com sucesso!\n", cmd.ID)

		case "5":
			cl.visualizarRequests(false)
			input.ReadString('\n')
		case "6":
			cl.visualizarRequests(true)
			input.ReadString('\n')
		case "7":
			fmt.Println("Saindo...")
			os.Exit(0)
		default:
			fmt.Println("Opção inválida!")
		}
	}
}

func limparTela() {
	fmt.Print("\033[H\033[2J")
}
