package main

import (
	"log"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

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
		SetClientID(n.Id + "-client").
		SetCleanSession(false).
		SetAutoReconnect(true)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Printf("[%s] Cliente MQTT conectado ao broker local", n.Id)

		// re-subscribe após reconexão
		c.Subscribe("drone/request", 1, func(_ pahomqtt.Client, m pahomqtt.Message) {
		})
	})

	opts.SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
		log.Printf("[%s] Conexão MQTT perdida: %v — reconectando...", n.Id, err)
	})

	for {
		client := pahomqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("[%s] Aguardando broker MQTT subir: %v", n.Id, token.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}
		n.mqttMu.Lock()
		n.mqtt = client
		n.mqttMu.Unlock()
		break
	}

}
