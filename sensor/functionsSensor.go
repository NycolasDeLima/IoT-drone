package main

import (
	"fmt"
	"math/rand"
)

func mudarEstado(tipoSensor string) string {

	var estados []string

	switch tipoSensor {
	case "bpm":
		estados = []string{"repouso", "atividade", "taquicardia", "bradicardia"}
	case "spo2":
		estados = []string{"normal", "leve", "moderado", "critico"}
	}

	return estados[rand.Intn(len(estados))]
}

func ajustarBPM(atual int, estado string) int {
	var alvo int

	switch estado {
	case "repouso":
		alvo = 70
	case "atividade":
		alvo = 110
	case "taquicardia":
		alvo = 140
	case "bradicardia":
		alvo = 50
	}

	// aproxima suavemente do alvo
	if atual < alvo {
		atual += rand.Intn(3)
	} else if atual > alvo {
		atual -= rand.Intn(3)
	}

	// pequeno ruído
	atual += rand.Intn(3) - 1

	return limitar(atual, 40, 180)
}

func ajustarSpO2(atual int, estado string) int {
	var alvo int

	switch estado {
	case "normal":
		alvo = 98
	case "leve":
		alvo = 93
	case "moderado":
		alvo = 88
	case "critico":
		alvo = 82
	}

	// aproxima suavemente do alvo
	if atual < alvo {
		atual += rand.Intn(2)
	} else if atual > alvo {
		atual -= rand.Intn(2)
	}

	// pequeno ruído
	atual += rand.Intn(3) - 1

	return limitar(atual, 70, 100)
}

func limitar(valor, min, max int) int {
	if valor < min {
		return min
	}
	if valor > max {
		return max
	}
	return valor
}

func exibirPainel(tipo, id string, dado int, estado string, status string) {
	limparTela()

	fmt.Println("====================================")
	fmt.Println("        MONITOR DE SENSOR")
	fmt.Println("====================================")
	fmt.Printf("Tipo do Sensor : %s\n", tipo)
	fmt.Printf("ID             : %s\n", id)
	fmt.Printf("Estado         : %s\n", estado)
	fmt.Printf("Status         : %s\n", status)
	fmt.Println("------------------------------------")

	switch tipo {
	case "bpm":
		fmt.Printf("Frequência Cardíaca: %d BPM\n", dado)
	case "spo2":
		fmt.Printf("Oxigenação (SpO2): %d%%\n", dado)
	}

	fmt.Println("====================================")
}

func limparTela() {
	fmt.Print("\033[H\033[2J")
}
