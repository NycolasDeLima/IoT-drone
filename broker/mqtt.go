package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/raft"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// inicia servidor MQTT
func startBroker(mqttPort string, id string) *mqtt.Server {

	server := mqtt.New(nil)

	_ = server.AddHook(new(auth.AllowHook), nil)

	tcp := listeners.NewTCP(listeners.Config{
		ID:      id,
		Address: mqttPort,
	})
	err := server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Println("MQTT Broker started on " + mqttPort)
	return server
}

// Conecta ao broker MQTT local e mantém a conexão, tentando reconectar em caso de falha
func (n *Node) connectMQTTBroker(mqttPort string) {

	brokerURL := "tcp://localhost" + mqttPort

	opts := pahomqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(n.ID + "-client").
		SetCleanSession(false).
		SetAutoReconnect(true)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Printf("[%s] Cliente MQTT conectado ao broker local", n.ID)

		// re-subscribe após reconexão
		token := c.Subscribe("drone/requests", 1, nil)
		token.Wait()
	})

	opts.SetDefaultPublishHandler(func(c pahomqtt.Client, msg pahomqtt.Message) {
		topic := msg.Topic()

		payload := string(msg.Payload())

		switch topic {
		case "drone/requests":
			n.handleRequest(msg.Payload())

		default:
			log.Printf("[%s] %s\n", topic, payload)
		}
	})

	opts.SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
		log.Printf("[%s] Conexão MQTT perdida: %v — reconectando...", n.ID, err)
	})

	for {
		client := pahomqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("[%s] Aguardando broker MQTT subir: %v", n.ID, token.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}
		n.mqttMu.Lock()
		n.mqtt = client
		n.mqttMu.Unlock()
		break
	}

}

func (n *Node) handleRequest(payload []byte) {

	var msg map[string]interface{}
	json.Unmarshal(payload, &msg)

	cmd := Command{
		Type:      AddRequest,
		ID:        fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
		RequestID: msg["request_id"].(string),
		Priority:  int(msg["priority"].(float64)),
		Timestamp: time.Now().Unix(),
	}

	data, _ := json.Marshal(cmd)

	if n.Raft.State() == raft.Leader {

		future := n.Raft.Apply(data, 5*time.Second)
		if future.Error() != nil {
			log.Printf("[%s] Erro ao aplicar comando recebido via MQTT: %v", n.ID, future.Error())
		} else {
			log.Printf("[%s] Comando recebido via MQTT aplicado com sucesso", n.ID)

			// Mutex!
			go n.allocation()
		}

	} else {
		err := n.ToLeader(data)
		if err != nil {
			log.Printf("[%s] Erro ao encaminhar comando para o líder via MQTT: %v", n.ID, err)
		} else {
			log.Printf("[%s] Comando recebido via MQTT encaminhado para o líder com sucesso", n.ID)
		}
	}
}
