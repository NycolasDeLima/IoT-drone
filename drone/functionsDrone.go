package main

import "fmt"

func exibirPainel(tipo, id string, dado int, estado string, status string) {
	limparTela()

	fmt.Println("====================================")
	fmt.Println("        MONITOR DE DRONE")
	fmt.Println("====================================")
	fmt.Printf("ID             : %s\n", id)
	fmt.Printf("Tarefa         : %s\n", estado)
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
