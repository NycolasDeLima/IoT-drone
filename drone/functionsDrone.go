package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// ================= Constantes ====================
const (
	Free = "Livre"
	Busy = "Ocupado"

	TaskCompleted = "TASK_COMPLETED"

	Heartbeat = "DRONE_HEARTBEAT"

	AddDrone    = "ADD_DRONE"
	RemoveDrone = "REMOVE_DRONE"

	Error = "ERROR"
)

// ================= Sructs ====================
type Mensagem struct {
	Tipo    string `json:"tipo"`
	ID      string `json:"id"`
	Dado    string `json:"dado"`
	Request string `json:"request"`
	MsgID   string `json:"msgid"`
}

// ================= Funções ====================
func (d *Drone) executarTarefa(msg Mensagem) {

	d.State = Busy

	d.Task = msg.Request

	d.SetorTask = msg.Dado

	d.ClientTask = msg.ID

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
		Tipo:    TaskCompleted,
		ID:      d.ID,
		Request: msg.Request,
		Dado:    msg.ID,
		MsgID:   fmt.Sprintf("%s-%d", d.ID, time.Now().UnixNano()),
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
		fmt.Printf("Setor da Tarefa: %s\n", d.SetorTask)
		fmt.Printf("Cliente da Tarefa: %s\n", d.ClientTask)
	} else {
		fmt.Printf("Tarefa Atual   : Nenhuma\n")
	}
	fmt.Printf("Estado         : %s\n", d.State)
	if d.Conected {
		fmt.Printf("Broker Conectado: %s\n", d.Setor)
	} else {
		fmt.Printf("Broker Desconectado \n")
		fmt.Printf("%s\n", d.conMsg)
	}
	fmt.Println("------------------------------------")

	fmt.Println("====================================")
}

func limparTela() {
	fmt.Print("\033[H\033[2J")
}

func (d *Drone) handleMensagemMQTT(c pahomqtt.Client, msg pahomqtt.Message) {

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
				Tipo:    Error,
				ID:      d.ID,
				Request: msgT.Request,
				Dado:    msgT.ID,
				MsgID:   fmt.Sprintf("%s-%d", d.ID, time.Now().UnixNano()),
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
		d.executarTarefa(msgT)

	default:
		fmt.Printf("[%s] %s\n", topic, payload)
	}
}

func (d *Drone) reconectarComFailover() {
	maxRetries := 3
	delayEntreTentativas := 5 * time.Second

	defer func() {
		d.mu.Lock()
		d.reconnecting = false
		d.mu.Unlock()
	}()

	// Se não houver brokers na lista, não tenta reconectar
	if len(d.Brokers) == 0 {
		d.conMsg = "Nenhum broker alternativo disponível"
		return
	}

	for !d.Conected {

		for brokerIdx := 0; brokerIdx < len(d.Brokers); brokerIdx++ {

			id, broker, err := splitPeer(d.Brokers[brokerIdx])
			if err != nil {
				d.conMsg = fmt.Sprintf("Erro ao processar broker %s: %v\n", d.Brokers[brokerIdx], err)
				continue
			}

			for tentativa := 0; tentativa < maxRetries; tentativa++ {

				d.conMsg = fmt.Sprintf("Tentando conectar ao broker %s (tentativa %d/%d)\n", broker, tentativa+1, maxRetries)

				if d.ConectarBroker(id, broker) {
					d.conMsg = fmt.Sprintf("Reconectado com sucesso ao broker: %s\n", broker)
					d.Conected = true
					return
				}

				// Aguarda antes de tentar novamente
				time.Sleep(delayEntreTentativas)
			}

			d.conMsg = fmt.Sprintf("Falha ao conectar ao broker %s, tentando próximo...\n", broker)
		}

		d.conMsg = "Falha ao reconectar em todos os brokers alternativos"
	}

}

func (d *Drone) ConectarBroker(idBroker string, brokerURL string) bool {

	opts := pahomqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(d.ID + "-Drone").
		SetCleanSession(false).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {

		addMsg := Mensagem{
			Tipo:  AddDrone,
			ID:    d.ID,
			Dado:  "Conectado",
			MsgID: fmt.Sprintf("%s-%d", d.ID, time.Now().UnixNano()),
		}

		addPayload, _ := json.Marshal(addMsg)

		d.Conected = true
		token := c.Subscribe("drone/tasks/"+d.ID, 1, nil)
		token.Wait()
		token = c.Publish("drone/status", 1, false, addPayload)
		token.Wait()

	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		log.Printf("Drone desconectado do broker %s: %v\n", brokerURL, err)
		d.Conected = false

		d.mu.Lock()
		if d.reconnecting {
			d.mu.Unlock()
			return
		}

		d.reconnecting = true
		d.mu.Unlock()
		go d.reconectarComFailover()
	})

	opts.SetDefaultPublishHandler(d.handleMensagemMQTT)

	if d.Client != nil && d.Client.IsConnected() {
		d.Client.Disconnect(250)
	}

	client := pahomqtt.NewClient(opts)
	token := client.Connect()

	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return false
	}

	// Atualiza o cliente principal
	d.Client = client
	d.Setor = idBroker
	d.Conected = true
	return true
}

func splitPeer(peer string) (string, string, error) {

	parts := strings.Split(peer, "=")

	if len(parts) != 2 {
		return "", "", fmt.Errorf("Peer inválido: %s", peer)
	}

	id := parts[0]

	ip := parts[1]

	return id, ip, nil
}
