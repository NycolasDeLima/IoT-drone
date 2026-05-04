package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	Follower  = "follower"
	Leader    = "leader"
	Candidate = "candidate"
)

type Request struct {
	ID        string
	Priority  int
	Timestamp int64
}

type Command struct {
	Type      string `json:"type"` // ADD_REQUEST | ALLOCATE
	RequestID string `json:"request_id"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
	DroneID   string `json:"drone_id"`
}

type LogEntry struct {
	Term    int
	Index   int
	Command Command
}

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
	Entry        *LogEntry `json:"entry,omitempty"`
	PrevLogIndex int       `json:"prev_log_index"`
	PrevLogTerm  int       `json:"prev_log_term"`
	LeaderCommit int       `json:"leader_commit"`
	Ack          bool      `json:"ack,omitempty"`
}

type Node struct {
	Id   string
	Port string
	Peer []string

	connMu    sync.Mutex
	Conns     map[string]net.Conn
	PeerAddrs map[string]string

	mqttMu sync.Mutex
	mqtt   pahomqtt.Client
}

func main() {

	// Parâmetros: id, mqttPort, tcpPort, peer1, peer2, ...
	var (
		id       string
		mqttPort string
		tcpPort  string
		peer     []string
	)

	if len(os.Args) < 4 {
		id = "1"
		mqttPort = ":1883"
		tcpPort = ":5000"
		peer = []string{"192.168.1.5:1884"}

	} else {
		id = os.Args[1]
		mqttPort = ":" + os.Args[2]
		tcpPort = ":" + os.Args[3]
		peer = os.Args[4:]
	}

	node := &Node{
		Id:   id,
		Peer: peer,
		Port: tcpPort,

		Conns:     make(map[string]net.Conn),
		PeerAddrs: make(map[string]string),
	}

	mqttServer := startBroker(mqttPort, id)

	go node.startTcpServer(tcpPort)
	go node.connectToPeers()

	time.Sleep(1 * time.Second)

	go node.connectMQTTBroker(mqttPort)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down broker...")
	mqttServer.Close()
}
