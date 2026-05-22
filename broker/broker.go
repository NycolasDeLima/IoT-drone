package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/raft"
)

// Estrura padrão do Nó
type Node struct {
	ID       string   // ID do nó
	IP       string   // Endereço IP do Nó
	RaftPort string   // Porta para comunicação do Raft
	TcpPort  string   // Porta para comunicação TCP
	Peer     []string // Lista de todos os Nós do sistema (ID=IP:PortaRaft:PortaTCP)

	Raft *raft.Raft // Estrutura para o raft
	FSM  *FSM       // Região Crítica (FSM)

	mqttMu sync.Mutex
	mqtt   pahomqtt.Client // Cliente MQTT

	allocating   bool // Está alocando
	allocationMu sync.Mutex
	allocationCh chan AllocationRequests // Comunicação entre Raft e MQTT
}

func main() {

	// Variáveis de configuração
	var (
		id        string
		ip        string
		tcpPort   string
		mqttPort  string
		raftPort  string
		firstPeer string
		peer      []string
	)

	if len(os.Args) < 6 {
		id = "1"
		ip = "0.0.0.0"
		mqttPort = ":1883"
		raftPort = ":5000"
		tcpPort = ":6000"
		firstPeer = ""
		peer = []string{}

	} else {
		id = os.Args[1]
		ip = os.Args[2]
		mqttPort = ":" + os.Args[3]
		raftPort = ":" + os.Args[4]
		tcpPort = ":" + os.Args[5]
		firstPeer = os.Args[6]
		peer = os.Args[7:]
	}

	// Configura o Nó
	node := &Node{
		ID:           id,
		IP:           ip,
		Peer:         peer,
		RaftPort:     raftPort,
		TcpPort:      tcpPort,
		allocationCh: make(chan AllocationRequests, 100),
	}

	time.Sleep(1 * time.Second)

	mqttServer := startBroker(mqttPort, id)

	time.Sleep(1 * time.Second)

	node.connectMQTTBroker(mqttPort)

	time.Sleep(1 * time.Second)

	go node.startTcpServer()

	time.Sleep(1 * time.Second)

	node.setupRaft()

	// Primeiro Nó a subir deve criar Cluster
	// Demais Nós firstPeer = "f"
	if firstPeer == "t" {

		node.Raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(node.ID),
					Address: raft.ServerAddress(node.IP + node.RaftPort),
				},
			},
		})
	}

	go node.addPeers()

	go node.listenAllocations()

	go node.monitorPendingRequests()

	go func() {
		for {
			time.Sleep(30 * time.Second)

			if node.Raft.State() == raft.Leader {
				node.FSM.cleanupProcessed(60)
			}
		}
	}()

	go node.cleanupDrones()

	go node.shareStatus()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down broker...")
	mqttServer.Close()
}

// Separa Partes das informações dos Nós
// ID, IP, RaftPort, TcpPort,
func splitPeer(peer string) (string, string, string, string, error) {

	parts := strings.Split(peer, "=")

	if len(parts) != 2 {
		return "", "", "", "", fmt.Errorf("Peer inválido: %s", peer)
	}

	id := parts[0]

	parts = strings.Split(parts[1], ":")

	if len(parts) != 3 {
		return "", "", "", "", fmt.Errorf("Endereço inválido: %s", parts[1])
	}

	ip := parts[0]
	raftPort := parts[1]
	tcpPort := parts[2]

	return id, ip, raftPort, tcpPort, nil
}
