package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/raft"
)

const (
	Follower  = "follower"
	Leader    = "leader"
	Candidate = "candidate"
)


type Message struct {
	Type string `json:"type"`

	SenderID   string `json:"sender_id"`
	SenderPort string `json:"sender_port"`

	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
	VoteGranted  bool   `json:"vote_granted"`
	LeaderID     string `json:"leader_id"`

	// log replication
	PrevLogIndex int       `json:"prev_log_index"`
	PrevLogTerm  int       `json:"prev_log_term"`
	LeaderCommit int       `json:"leader_commit"`
	Ack          bool      `json:"ack,omitempty"`
}

type Node struct {
	ID   string
	Port string
	Peer []string

	Raft *raft.Raft
	FSM *FSM

	mqttMu sync.Mutex
	mqtt   pahomqtt.Client
}

func main() {

	// Parâmetros: id, mqttPort, tcpPort, peer1, peer2, ...
	var (
		id       string
		mqttPort string
		raftPort  string
		peer     []string
	)

	if len(os.Args) < 4 {
		id = "1"
		mqttPort = ":1883"
		raftPort = ":5000"
		peer = []string{"192.168.1.5:1884"}

	} else {
		id = os.Args[1]
		mqttPort = ":" + os.Args[2]
		raftPort = ":" + os.Args[3]
		peer = os.Args[4:]
	}

	node := &Node{
		ID:   id,
		Peer: peer,
		Port: raftPort,


	}

	mqttServer := startBroker(mqttPort, id)

	go node.startTcpServer(raftPort)
	go node.connectToPeers()

	time.Sleep(1 * time.Second)

	go node.connectMQTTBroker(mqttPort)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down broker...")
	mqttServer.Close()
}
