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

type Node struct {
	ID       string
	IP       string
	RaftPort string
	TcpPort  string
	Peer     []string

	Raft *raft.Raft
	FSM  *FSM

	mqttMu sync.Mutex
	mqtt   pahomqtt.Client
}

func main() {

	// Parâmetros: id, ip, mqttPort, tcpPort, peer1, peer2, ...
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

	node := &Node{
		ID:       id,
		IP:       ip,
		Peer:     peer,
		RaftPort: raftPort,
		TcpPort:  tcpPort,
	}

	node.setupRaft()

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

	time.Sleep(1 * time.Second)

	go node.startTcpServer()

	time.Sleep(1 * time.Second)

	mqttServer := startBroker(mqttPort, id)

	time.Sleep(1 * time.Second)

	go node.connectMQTTBroker(mqttPort)

	/*

		go func() {

			for {

				node.allocation()
				time.Sleep(300 * time.Millisecond)
			}

		}()

	*/

	go func() {
		for {
			time.Sleep(30 * time.Second)

			if node.Raft.State() == raft.Leader {
				node.FSM.cleanupProcessed(60)
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down broker...")
	mqttServer.Close()
}

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
