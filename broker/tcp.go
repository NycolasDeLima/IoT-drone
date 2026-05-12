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

const (
	Forward = "FORWARD"
	Ack     = "ACK"
	Error   = "ERROR"
)

type Mensagem struct {
	Type    string          `json:"type"` // FORWARD | ACK | ERROR
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
	Error   string          `json:"error,omitempty"`
}

func (n *Node) startTcpServer() {
	// Implementação do servidor TCP para comunicação entre nós

	listenner, err := net.Listen("tcp", n.TcpPort)
	if err != nil {
		log.Fatalf("[%s][TCP] Erro ao iniciar servidor TCP: %v", n.ID, err)
	}

	log.Printf("[%s][TCP] Servidor TCP iniciado na porta %s", n.ID, n.TcpPort)

	for {
		conn, err := listenner.Accept()
		if err != nil {
			log.Printf("[%s][TCP]Erro ao aceitar conexão TCP: %v", n.ID, err)
			continue
		}

		go n.handleTcpAccept(conn)
	}

}

func (n *Node) handleTcpAccept(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewReader(conn)

	log.Printf("[%s][TCP] Conexão aceita %s", n.ID, conn.RemoteAddr().String())

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

	switch msg.Type {
	case Forward:
		log.Printf("[%s][TCP] Comando recebido para encaminhamento: %s", n.ID, string(msg.Payload))
		n.handleForward(msg, conn)
	default:
		log.Printf("[%s][TCP] Tipo de mensagem TCP desconhecida: %s", n.ID, msg.Type)
	}

}

func (n *Node) handleForward(msg Mensagem, conn net.Conn) {

	if n.Raft.State() != raft.Leader {
		response := Mensagem{
			Type:  Error,
			ID:    msg.ID,
			Error: "Este nó não é o líder, comando deve ser encaminhado para o líder",
		}
		respData, _ := json.Marshal(response)
		conn.Write(append(respData, '\n'))
		return
	}

	future := n.Raft.Apply(msg.Payload, 5*time.Second)

	if future.Error() != nil {
		log.Printf("[%s] Erro ao aplicar comando recebido via TCP: %v", n.ID, future.Error())
		response := Mensagem{
			Type:  Error,
			ID:    msg.ID,
			Error: future.Error().Error(),
		}
		respData, _ := json.Marshal(response)
		conn.Write(append(respData, '\n'))
		return
	} else {
		log.Printf("[%s] Comando recebido via TCP aplicado com sucesso", n.ID)
		n.allocation()
	}

	response := Mensagem{
		Type:  Ack,
		ID:    msg.ID,
		Error: "",
	}
	respData, _ := json.Marshal(response)
	conn.Write(append(respData, '\n'))

	log.Printf("[%s] Comando encaminhado processado e aplicado com sucesso: %s", n.ID, string(msg.Payload))

}

func (n *Node) ToLeader(data []byte) error {

	var leaderIP string

	leader, leaderID := n.Raft.LeaderWithID()

	if leader == "" {
		log.Printf("[%s] Nenhum líder encontrado", n.ID)
		return fmt.Errorf("Nenhum líder encontrado")
	}

	log.Printf("[%s] Leader: '%s'", n.ID, leader)

	future := n.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		log.Printf("[%s] Erro ao obter configuração do cluster: %v", n.ID, err)
		return fmt.Errorf("Erro ao obter configuração do cluster: %v", err)
	}

	log.Printf("[%s] Líder identificado: %s (ID: %s)", n.ID, leader, leaderID)

	for _, p := range n.Peer {

		id, ip, _, tcpPort, err := splitPeer(p)
		if err != nil {
			log.Printf("[%s] Erro ao processar peer %s: %v", n.ID, p, err)
			continue
		}

		log.Printf("[%s] Verificando peer %s (Raft: %s)", n.ID, p, id)

		if id == string(leaderID) {

			leaderIP = net.JoinHostPort(ip, tcpPort)
			break

		}

	}

	env := Mensagem{
		Type:    Forward,
		ID:      fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
		Payload: data,
	}

	if leaderIP == "" {
		log.Printf("[%s] Líder não encontrado entre os peers", n.ID)
		return fmt.Errorf("Líder não encontrado entre os peers")
	}

	for i := 0; i < 3; i++ {

		conn, err := net.DialTimeout("tcp", leaderIP, 2*time.Second)
		if err != nil {
			log.Printf("[%s] Erro ao conectar ao líder: %v", n.ID, err)
			continue
		}

		conn.SetDeadline(time.Now().Add(3 * time.Second))

		msgData, _ := json.Marshal(env)

		_, err = conn.Write(append(msgData, '\n'))
		if err != nil {
			log.Printf("[%s] Erro ao enviar dados para o líder: %v", n.ID, err)
			conn.Close()
			continue
		}

		responseData, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			log.Printf("[%s] Erro ao ler resposta do líder: %v", n.ID, err)
			conn.Close()
			continue
		}

		var response Mensagem
		err = json.Unmarshal(responseData, &response)
		if err != nil {
			log.Printf("[%s] Erro ao processar resposta do líder: %v", n.ID, err)
			conn.Close()
			continue
		}

		switch response.Type {
		case Ack:
			log.Printf("[%s] Comando encaminhado para o líder com sucesso", n.ID)
			conn.Close()
			return nil
		case Error:
			log.Printf("[%s] Erro do líder ao processar comando: %v", n.ID, response.Error)
			conn.Close()
			return fmt.Errorf("Erro do líder: %v", response.Error)
		default:
			log.Printf("[%s] Resposta inesperada do líder: %s", n.ID, response.Type)
			conn.Close()
			continue
		}

	}

	return fmt.Errorf("Falha ao encaminhar comando para o líder após múltiplas tentativas")
}

func (n *Node) allocation() {

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

		for {
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
				log.Printf("[%s] Erro ao aplicar comando de alocação: %v", n.ID, future.Error())
			}

			allocate, ok := future.Response().(bool)
			if !ok || !allocate {
				return
			} else {
				log.Printf("[%s] Comando de alocação aplicado com sucesso", n.ID)
			}
		}

	}()

}
