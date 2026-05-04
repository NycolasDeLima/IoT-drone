package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"time"
)

func (n *Node) startTcpServer(tcpPort string) {
	// Implementação do servidor TCP para comunicação entre nós

	listenner, err := net.Listen("tcp", tcpPort)
	if err != nil {
		log.Fatalf("[%s] Erro ao iniciar servidor TCP: %v", n.Id, err)
	}

	log.Printf("[%s] Servidor TCP iniciado na porta %s", n.Id, tcpPort)

	for {
		conn, err := listenner.Accept()
		if err != nil {
			log.Printf("[%s] Erro ao aceitar conexão TCP: %v", n.Id, err)
			continue
		}

		go n.handleTcpAccept(conn)
	}

}

func (n *Node) handleTcpAccept(conn net.Conn) {

	reader := bufio.NewReader(conn)

	data, err := reader.ReadBytes('\n')
	if err != nil {
		conn.Close()
		return
	}

	var handshake Message
	if err := json.Unmarshal(data, &handshake); err != nil || handshake.Type != "HANDSHAKE" {
		log.Printf("[%s] Handshake inválido — conexão recusada", n.Id)
		conn.Close()
		return
	}

	resp, _ := json.Marshal(Message{Type: "HANDSHAKE", SenderID: n.Id})
	conn.Write(append(resp, '\n'))

	n.connMu.Lock()
	n.Conns[handshake.SenderID] = conn
	ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	n.PeerAddrs[handshake.SenderID] = net.JoinHostPort(ip, handshake.SenderPort)
	n.connMu.Unlock()

	log.Printf("[%s] Peer %s conectado", n.Id, handshake.SenderID)

	n.handleTcpConnection(handshake.SenderID, conn)

}

func (n *Node) connectToPeers() {
	// Implementação para conectar aos peers listados em n.Peer

	for _, p := range n.Peer {
		go func(addr string) {
			for {
				n.tryConnectPeer(addr)
				time.Sleep(2 * time.Second)
			}
		}(p)

	}

}

func (n *Node) tryConnectPeer(addr string) {

	// verifica se já há conexão ativa para este endereço
	n.connMu.Lock()
	for _, peerAddr := range n.PeerAddrs {
		if peerAddr == addr {
			n.connMu.Unlock()
			return
		}
	}
	n.connMu.Unlock()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {

		return
	}

	handshake, _ := json.Marshal(Message{Type: "HANDSHAKE",
		SenderID: n.Id, SenderPort: n.Port})
	conn.Write(append(handshake, '\n'))

	reader := bufio.NewReader(conn)
	data, err := reader.ReadBytes('\n')
	if err != nil {
		conn.Close()

		return
	}

	var resp Message
	if err := json.Unmarshal(data, &resp); err != nil || resp.Type != "HANDSHAKE" {
		log.Printf("[%s] Handshake inválido com %s", n.Id, addr)
		conn.Close()

		return
	}

	n.connMu.Lock()
	_, jaExiste := n.Conns[resp.SenderID]
	if jaExiste {
		if n.Id > resp.SenderID {
			// mantém a atual, fecha nova
			conn.Close()
			n.connMu.Unlock()
			return
		} else {
			// substitui pela nova
			oldConn := n.Conns[resp.SenderID]
			oldConn.Close()
			n.Conns[resp.SenderID] = conn
			n.PeerAddrs[resp.SenderID] = addr
			n.connMu.Unlock()
			log.Printf("[%s] Conexão duplicada com %s — descartando", n.Id, resp.SenderID)
		}
	}

	log.Printf("[%s] Conectado ao peer %s (%s)", n.Id, resp.SenderID, addr)
	n.handleTcpConnection(resp.SenderID, conn)

	// handleConn retornou — peer desconectou, tenta reconectar
	log.Printf("[%s] Reconectando a %s...", n.Id, addr)
}

func (n *Node) handleTcpConnection(peerID string, conn net.Conn) {
	// Implementação para lidar com mensagens recebidas de outros nós

	defer func() {
		n.connMu.Lock()
		if n.Conns[peerID] == conn {
			delete(n.Conns, peerID)
		}

		n.connMu.Unlock()
		conn.Close()
		log.Printf("[%s] Peer %s desconectado", n.Id, peerID)
	}()

	var msg Message
	reader := bufio.NewReader(conn)

	for {
		buffer, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		err = json.Unmarshal([]byte(buffer), &msg)
		if err != nil {
			log.Println("JSON inválido:", err)
			return
		}

		log.Printf("[%s] Mensagem recebida de %s: %+v\n", n.Id, conn.RemoteAddr(), msg)

	}

}

func (n *Node) broadcast(msg Message) {
	n.connMu.Lock()
	peers := make([]string, len(n.Peer))
	copy(peers, n.Peer)
	n.connMu.Unlock()

	for _, p := range peers {
		go n.send(p, msg)
	}
}

func (n *Node) send(peerID string, msg Message) (Message, bool) {

	n.connMu.Lock()
	conn, ok := n.Conns[peerID]
	n.connMu.Unlock()

	if !ok {

		return Message{}, false
	}

	data, _ := json.Marshal(msg)
	data = append(data, '\n')

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	_, err := conn.Write(data)
	if err != nil {
		return Message{}, false
	}

	reader := bufio.NewReader(conn)
	respData, err := reader.ReadBytes('\n')
	conn.SetDeadline(time.Time{})
	if err != nil {
		return Message{}, false
	}

	var resp Message
	if err := json.Unmarshal(respData, &resp); err != nil {
		return Message{}, false
	}
	return resp, true

}
