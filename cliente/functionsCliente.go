package main

import (
	"sync"
	"time"
)

var (
	sensores = make(map[string]Sensor)

	loc, _ = time.LoadLocation("America/Sao_Paulo")

	muS sync.RWMutex
)

const (
	Allocate   = "ALLOCATE"
	AddRequest = "ADD_REQUEST"
)

// ================= Protocolo de Comunicação ====================

type Mensagem struct {
	Tipo    string `json:"tipo"`
	ID      string `json:"id"`
	Dado    string `json:"dado"`
	Request string `json:"request"`
}

type Sensor struct {
	Tipo        string `json:"tipo"`
	ID          string `json:"id"`
	Dado        string `json:"dado"`
	UltimoVisto time.Time
}

type Command struct {
	Type      string `json:"type"` // ADD_REQUEST | ALLOCATE
	ID        string `json:"id"`
	RequestID string `json:"request_id"`
	Priority  int    `json:"priority"`
	Timestamp int64  `json:"timestamp"`
	DroneID   string `json:"drone_id"`
}

func removerSensor() {
	for {
		time.Sleep(5 * time.Second)

		muS.Lock()
		for key, sensor := range sensores {
			if time.Since(sensor.UltimoVisto) > 5*time.Second {
				sensor.Dado = "offline"
				sensores[key] = sensor
			}
		}

		muS.Unlock()
	}
}
