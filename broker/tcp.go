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
	Forward = "FORWARD"
	Ack     = "ACK"
	Error   = "ERROR"
)

// ================= Struct ====================

type Mensagem struct {
	Type    string          `json:"type"` // FORWARD | ACK | ERROR
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
	Error   string          `json:"error,omitempty"`
}

// ================= TCP ====================

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
			log.Printf("[%s][TCP] Erro ao aceitar conexão TCP: %v", n.ID, err)
			continue
		}

		go n.handleTcpAccept(conn)
	}

}

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

	switch msg.Type {
	case Forward:
		//log.Printf("[%s][TCP] Comando recebido para encaminhamento: %s", n.ID, msg)
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
		log.Printf("[%s][TCP] Comando recebido aplicado com sucesso: \n%s", n.ID, future.Response().(raftResponse).msg)
		n.allocation()
	}

	response := Mensagem{
		Type:  Ack,
		ID:    msg.ID,
		Error: "",
	}
	respData, _ := json.Marshal(response)
	conn.Write(append(respData, '\n'))

}

func (n *Node) ToLeader(data []byte) {

	var leaderIP string

	leader, leaderID := n.Raft.LeaderWithID()

	if leader == "" {
		log.Printf("[%s][TCP] Nenhum líder encontrado", n.ID)
		return
	}

	future := n.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		log.Printf("[%s][TCP] Erro ao obter configuração do cluster: %v", n.ID, err)
		return
	}

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

	env := Mensagem{
		Type:    Forward,
		ID:      fmt.Sprintf("%s-%d", n.ID, time.Now().UnixNano()),
		Payload: data,
	}

	if leaderIP == "" {
		log.Printf("[%s][TCP] Líder não encontrado entre os peers", n.ID)
	}

	for i := 0; i < 3; i++ {

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
			log.Printf("[%s][TCP] Comando encaminhado para o líder com sucesso", n.ID)
			conn.Close()
			return
		case Error:
			log.Printf("[%s][TCP] Erro do líder ao processar comando: %v", n.ID, response.Error)
			conn.Close()
		default:
			log.Printf("[%s][TCP] Resposta inesperada do líder: %s", n.ID, response.Type)
			conn.Close()
			continue
		}

	}

	log.Printf("[%s][TCP] Falha ao encaminhar comando para o líder após múltiplas tentativas", n.ID)
}

// ================= Alocação de Requisições ====================
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
