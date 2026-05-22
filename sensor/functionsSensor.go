package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

// ================= Constantes ====================
const (

	// tipos de sensores
	radar = "radar"
	sonar = "sonar"

	nadaDetectado = "NADA DETECTADO"

	// estados radar
	embarcacaoSuspeita = "EMBARCAÇÃO SUSPEITA"
	rotaBloqueada      = "ROTA BLOQUEADA"
	embarcacaoDeriva   = "EMBARCAÇÃO À DERIVA"
	trafegoIntenso     = "TRAFEGO INTENSO"

	// estados sonar
	interferencia        = "INTERFERÊNCIA"
	destrocos            = "DESTROÇOS"
	objetoDetectado      = "OBJETO DETECTADO"
	submersivelDetectado = "SUBMERSÍVEL DETECTADO"

	// requests radar
	identificarEmbarcacao = "IDENTIFICAR EMBARCAÇÃO" // 4
	verificarRota         = "VERIFICAR ROTA"         //2

	// requests sonar
	buscaResgate      = "BUSCA E RESGATE"    // 5
	investigarObjetos = "INVESTIGAR OBJETOS" // 3
	patrulhaAerea     = "PATRULHA AÉREA"     // 1

	// Adicionar Request
	AddRequest = "ADD_REQUEST"
)

// ================= Structs ====================

// Estrutura padrão de mensagens MQTT
type Mensagem struct {
	Tipo    string `json:"tipo"`    // Tipo de mensagem
	ID      string `json:"id"`      // ID do Dispositivo
	Dado    string `json:"dado"`    // Informação (ex: prioridade)
	Request string `json:"request"` // Requisição
	MsgID   string `json:"msgid"`   // ID único da mensagem
}

// Associa um evento a uma requisição e sua prioridade
type Evento struct {
	Estado   string
	Request  string
	Priority int
}

// ================= MQTT ====================

// Conecta o sensor ao broker MQTT
func (s *Sensor) conectarMQTT(serverIP string) {

	willMsg := Mensagem{
		Tipo: s.Tipo,
		ID:   s.SensorID,
		Dado: "offline",
	}

	willPayload, _ := json.Marshal(willMsg)

	opts := pahomqtt.NewClientOptions().
		AddBroker(serverIP).
		SetClientID(s.SensorID).
		SetCleanSession(false).
		SetWill(
			"sensors/heartbeat/"+s.SensorID,
			string(willPayload),
			1,
			false,
		)

	opts.SetOnConnectHandler(func(c pahomqtt.Client) {
		log.Println("Sensor connected to broker")
		s.Connected = true
	})

	opts.SetConnectionLostHandler(func(c pahomqtt.Client, err error) {
		log.Printf("Sensor connection lost: %v", err)
		s.Connected = false
		s.exibirPainel()
	})

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	s.Client = client
	s.Connected = true

}

// ================= Simulação ====================

// Simula detecção de eventos pelo sensor
func (s *Sensor) simularDeteccao() {

	// Se já existe evento ativo
	if s.EventoAtivo {

		s.TempoEstado--

		// terminou o evento
		if s.TempoEstado <= 0 {

			s.Estado = nadaDetectado
			s.EventoAtivo = false

			return
		}

		return
	}

	// chance de gerar novo evento
	if rand.Float64() < 0.50 {

		eventos := []string{}

		for estado := range s.SensorEventos {
			eventos = append(eventos, estado)
		}

		novoEstado := eventos[rand.Intn(len(eventos))]

		s.Estado = novoEstado
		s.EventoAtivo = true

		// evento dura entre 5 e 15 ciclos
		s.TempoEstado = rand.Intn(10) + 5

		// envia request
		s.enviarRequest()
	}
}

// Publica uma requisição no broker MQTT
func (s *Sensor) enviarRequest() {

	evento := s.SensorEventos[s.Estado]

	msg := Mensagem{
		Tipo:    AddRequest,
		ID:      s.SensorID,
		Dado:    strconv.Itoa(evento.Priority),
		Request: evento.Request,
		MsgID:   fmt.Sprintf("%s-%d", s.ID, time.Now().UnixNano()),
	}

	payload, _ := json.Marshal(msg)

	token := s.Client.Publish(
		"drone/requests",
		1,
		false,
		payload,
	)

	token.Wait()

	log.Printf(
		"[SENSOR %s] Evento detectado: %s -> %s",
		s.ID,
		s.Estado,
		evento.Request,
	)
}

// ================= Interface ====================

// Exibe painel do sensor no terminal
func (s *Sensor) exibirPainel() {
	limparTela()

	fmt.Println("====================================")
	fmt.Println("        MONITOR DE SENSOR")
	fmt.Println("====================================")
	fmt.Printf("Tipo do Sensor : %s\n", s.Tipo)
	fmt.Printf("ID             : %s\n", s.ID)
	fmt.Printf("Estado         : %s\n", s.Estado)

	if s.Connected {
		fmt.Printf("Status         : Conectado ao Setor\n")
	} else {
		fmt.Printf("Status         : Desconectado do Setor\n")
	}
	fmt.Println("------------------------------------")

	fmt.Println("====================================")
}

// Limpa o terminal
func limparTela() {
	fmt.Print("\033[H\033[2J")
}
