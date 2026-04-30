package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

func main() {

	var (
		id    string
		port  string
		peers []string
	)

	if len(os.Args) < 3 {
		id = "1"
		port = ":5000"
		peers = nil

	} else {
		id = os.Args[1]
		port = os.Args[2]
		peers = os.Args[3:]
	}

	// ------- MQTT ----------

	server := mqtt.New(nil)

	_ = server.AddHook(new(auth.AllowHook), nil)

	tcp := listeners.NewTCP(listeners.Config{
		ID:      "tcp1",
		Address: ":1883",
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

	log.Println("MQTT Broker started on :1883")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down broker...")
	server.Close()
}
