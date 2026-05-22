package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// ================= Constantes ====================
const (

	// Tipo de Mensagem TCP
	Forward = "FORWARD"
	Ack     = "ACK"
	Error   = "ERROR"
)

// ================= Struct ====================

// Estrutura padrão de mensagens TCP
type Mensagem struct {
	Type    string          `json:"type"`            // Tipo de Mensagem TCP
	ID      string          `json:"id"`              // ID único da mensagem (ID + Time)
	Payload json.RawMessage `json:"payload"`         // Mensagem MQTT encaminhada
	Error   string          `json:"error,omitempty"` // Erro
}

// ================= TCP ====================

// Servidor TCP para comunicação entre nós
func (n *Node) startTcpServer() {

	listenner, err := net.Listen("tcp", n.TcpPort)
	if err != nil {
		log.Fatalf("[%s][TCP] Erro ao iniciar servidor TCP: %v", n.ID, err)
	}

	log.Printf("[%s][TCP] Servidor TCP iniciado na porta %s", n.ID, n.TcpPort)

	for {
		conn, err := listenner.Accept()
		if err != nil {
			log.Printf("[%s][TCP] Erro ao aceitar conexão TCP: %v", n.ID, err)
			continue
		}

		go n.handleTcpAccept(conn)
	}

}

// Lida com Mensagens TCP
func (n *Node) handleTcpAccept(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewReader(conn)

	data, err := scanner.ReadBytes('\n')
	if err != nil {
		log.Printf("[%s][TCP] Erro ao ler dados: %v", n.ID, err)
		return
	}

	var msg Mensagem
	err = json.Unmarshal(data, &msg)
	if err != nil {
		log.Printf("[%s][TCP] Erro ao processar mensagem: %v", n.ID, err)
		return
	}

	switch msg.Type { // Verifica se tipo de mensagem é válida
	case Forward:
		n.handleForward(msg, conn)
	default:
		log.Printf("[%s][TCP] Tipo de mensagem TCP desconhecida: %s", n.ID, msg.Type)
	}

}

// Lida com Mensagem TCP encaminhada para o líder
func (n *Node) handleForward(msg Mensagem, conn net.Conn) {

	if n.Raft.State() != raft.Leader { // Verifica se é líder
		response := Mensagem{
			Type:  Error,
			ID:    msg.ID,
			Error: "Este nó não é o líder, comando deve ser encaminhado para o líder",
		}
		respData, _ := json.Marshal(response)
		conn.Write(append(respData, '\n'))
		return
	}

	future := n.Raft.Apply(msg.Payload, 5*time.Second) // Aplica comando encaminhado

	// Verifica se comando foi bem sucedido
	if future.Error() != nil {
		log.Printf("[%s][TCP] Erro ao aplicar comando recebido: %v", n.ID, future.Error())

		response := Mensagem{
			Type:  Error,
			ID:    msg.ID,
			Error: future.Error().Error(),
		}

		respData, _ := json.Marshal(response)

		conn.Write(append(respData, '\n'))
		return

	} else {

		response, ok := future.Response().(raftResponse)

		if !ok || !response.applied {

			log.Printf("[%s][TCP] Erro ao aplicar comando recebido: %v", n.ID, response.msg)

			response := Mensagem{
				Type:  Error,
				ID:    msg.ID,
				Error: future.Error().Error(),
			}

			respData, _ := json.Marshal(response)

			conn.Write(append(respData, '\n'))
			return
		}

		log.Printf("[%s][TCP] Comando recebido aplicado com sucesso: \n%s", n.ID, future.Response().(raftResponse).msg)
		n.allocation() // Chama alocação quando chega mensagem
	}

	// Mensagem de confirmação
	response := Mensagem{
		Type:  Ack,
		ID:    msg.ID,
		Error: "",
	}
	respData, _ := json.Marshal(response)
	conn.Write(append(respData, '\n'))

}

// Encaminha mensagem MQTT para o líder
func (n *Node) ToLeader(data []byte) (string, bool) {

	var leaderIP string

	leader, leaderID := n.Raft.LeaderWithID() // Pega endereço do líder

	if leader == "" {

		return fmt.Sprintf("[%s][TCP] Nenhum líder encontrado", n.ID), false
	}

	future := n.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Sprintf("[%s][TCP] Erro ao obter configuração do cluster: %v", n.ID, err), false
	}

	// Procura líder na lista de nós
	for _, p := range n.Peer {

		id, ip, _, tcpPort, err := splitPeer(p)
		if err != nil {
			continue
		}

		if id == string(leaderID) {

			leaderIP = net.JoinHostPort(ip, tcpPort)
			break

		}

	}

	env := Mensagem{ // Mensagem para o líder
		Type:    Forward,
		ID:      fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
		Payload: data,
	}

	if leaderIP == "" {
		return fmt.Sprintf("[%s][TCP] Líder não encontrado entre os peers", n.ID), false
	}

	for i := 0; i < 3; i++ { // Tenta encaminhar mensagem para o líder 3 vezes

		conn, err := net.DialTimeout("tcp", leaderIP, 2*time.Second)
		if err != nil {
			log.Printf("[%s][TCP] Erro ao conectar ao líder: %v", n.ID, err)
			continue
		}

		conn.SetDeadline(time.Now().Add(3 * time.Second))

		msgData, _ := json.Marshal(env)

		_, err = conn.Write(append(msgData, '\n'))
		if err != nil {
			log.Printf("[%s][TCP] Erro ao enviar dados para o líder: %v", n.ID, err)
			conn.Close()
			continue
		}

		responseData, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			log.Printf("[%s][TCP] Erro ao ler resposta do líder: %v", n.ID, err)
			conn.Close()
			continue
		}

		var response Mensagem
		err = json.Unmarshal(responseData, &response)
		if err != nil {
			log.Printf("[%s][TCP] Erro ao processar resposta do líder: %v", n.ID, err)
			conn.Close()
			continue
		}

		switch response.Type {
		case Ack:
			conn.Close()
			return fmt.Sprintf("[%s][TCP] Comando encaminhado para o líder com sucesso", n.ID), true
		case Error:
			log.Printf("[%s][TCP] Erro do líder ao processar comando: %v", n.ID, response.Error)
			conn.Close()
		default:
			log.Printf("[%s][TCP] Resposta inesperada do líder: %s", n.ID, response.Type)
			conn.Close()
			continue
		}

	}

	return fmt.Sprintf("[%s][TCP] Falha ao encaminhar comando para o líder após múltiplas tentativas", n.ID), false
}

// ================= Alocação de Requisições ====================

// Manda comando de alocação para o Raft
func (n *Node) allocation() {

	// Verifica se loop de alocação já está rodando
	n.allocationMu.Lock()

	if n.allocating {
		n.allocationMu.Unlock()
		return
	}

	n.allocating = true

	n.allocationMu.Unlock()

	go func() {

		defer func() {
			n.allocationMu.Lock()
			n.allocating = false
			n.allocationMu.Unlock()
		}()

		for { // Loop roda enquanto houver Requisições e Drones livres
			if n.Raft.State() != raft.Leader {
				return
			}

			cmd := Command{
				Type: Allocate,
				ID:   fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
			}

			data, _ := json.Marshal(cmd)

			future := n.Raft.Apply(data, 5*time.Second)
			if future.Error() != nil {
				log.Printf("[%s][ALLOC] Erro ao aplicar comando de alocação: %v", n.ID, future.Error())
			}

			allocate, ok := future.Response().(raftResponse)
			if !ok || !allocate.applied {
				return
			} else {
				log.Printf("[%s][ALLOC] Comando de alocação aplicado com sucesso\n%s", n.ID, allocate.msg)
			}
		}

	}()

}
