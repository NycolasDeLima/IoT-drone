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
	ListarSensores  = "LISTAR SENSORES"
	ListarAtuadores = "LISTAR ATUADORES"
	AcaoAtuador     = "ACAO ATUADOR"
	VerDadoSensor   = "VER DADO SENSOR"
	RemoverInscrito = "REMOVER INSCRITO"
)

// ================= Protocolo de Comunicação ====================

type Mensagem struct {
	Tipo string `json:"tipo"`
	ID   string `json:"id"`
	Dado string `json:"dado"`
}

type Sensor struct {
	Tipo        string `json:"tipo"`
	ID          string `json:"id"`
	Dado        string `json:"dado"`
	UltimoVisto time.Time
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
