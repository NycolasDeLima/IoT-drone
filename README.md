# 🚁 Sistema Distribuído para Coordenação de Drones

## 1. 📌 Apresentação

Este projeto implementa uma arquitetura distribuída para coordenação de drones autônomos de monitoramento marítimo em um cenário inspirado no Estreito de Ormuz.

O sistema é composto por múltiplos setores marítimos independentes, responsáveis pelo gerenciamento de drones compartilhados. Cada setor opera através de um broker distribuído que coordena requisições, monitora drones e mantém sincronização de estado utilizando o algoritmo de consenso Raft.

A solução foi desenvolvida com foco em:

- ✅ Tolerância a falhas
- ✅ Coordenação distribuída
- ✅ Exclusão mútua
- ✅ Priorização de requisições
- ✅ Replanejamento automático
- ✅ Ausência de ponto único de falha

---

# ⚙️ 2. Funcionalidades

O sistema possui as seguintes funcionalidades:

- 🚁 Coordenação distribuída de drones
- 👑 Eleição automática de líder com Raft
- 🔄 Replicação de estado entre setores
- 📋 Fila de prioridade baseada em heap
- 🔒 Exclusão mútua na alocação de drones
- 🧾 Deduplicação de comandos
- 💓 Monitoramento de drones por heartbeat
- 🛠️ Recuperação automática de falhas
- 🔁 Replanejamento de requisições
- 📡 Comunicação assíncrona com MQTT
- 🐳 Execução distribuída com Docker

---


# 🏗️ 3. Arquitetura

A arquitetura é composta pelos seguintes componentes:

## 🖥️ Nós de setor

Responsáveis por:

- Receber requisições
- Gerenciar drones
- Manter estado distribuído
- Coordenar alocações

Os nós utilizam o algoritmo Raft para garantir consistência entre os nós.

## 🚁 Drones

Responsáveis por:

- Receber tarefas
- Executar monitoramento
- Enviar heartbeat periódico
- Informar conclusão das tarefas
- Reconectar em outro setor em casa de indisponibilidade

## 📟 Sensores

Sensores simulados geram eventos aleatórios como:

- 🚢 Embarcações suspeitas
- 🚦 Congestionamentos marítimos
- ❓ Objetos não identificados
- ⚠️ Falhas em rotas


## 💻 Cliente

Aplicação utilizada para:

- Visualizar estado do sistema
- Enviar requisições manualmente

---

# 🛠️ 4. Tecnologias Utilizadas

- 🟦 Go 1.25.1
- 🐳 Docker
- 📦 Docker Compose
- 📡 MQTT
- 🔌 TCP Sockets
- 👑 HashiCorp Raft
- 📨 Eclipse Paho MQTT
- 💾 BoltDB

---

# 🌐 5. Comunicação

## 📡 MQTT

Utilizado para comunicação assíncrona entre:

- Sensores
- Drones
- Clientes
- Brokers

O modelo publish/subscribe reduz o acoplamento entre os componentes.

## 🔌 TCP

Utilizado para:

- Comunicação interna do Raft
- Encaminhamento de comandos ao líder

Apenas o líder pode modificar o estado distribuído do sistema.

---

# 📁 6. Estrutura do projeto

```text
.
├── broker/
│   ├── broker.go
│   ├── raft.go
│   ├── mqtt.go
│   ├── tcp.go
│   ├── priority.go
│   └── Dockerfile
│
├── drone/
│   ├── drone.go
│   ├── functionsDrone.go
│   └── Dockerfile
│
├── sensor/
│   ├── sensor.go
│   ├── functionsSensor.go
│   └── Dockerfile
│
├── cliente/
│   ├── cliente.go
│   ├── functionsCliente.go
│   └── Dockerfile
│
├── docker-compose.yml
└── README.md
```

# 🚀 7. Como executar


## Pré-requisitos

- Docker instalado

---

## 🔹 1. Clonar o Repositório

```bash
git clone https://github.com/NycolasDeLima/IoT-drone.git
```

## 🔹 2. Build da imagem

- **Setor**:
```bash
cd broker
docker build -t broker .
```

- **Cliente**:
```bash
cd cliente
docker build -t cliente .
```

- **Drone**:
```bash
cd drone
docker build -t drone .
```

- **Sensor**:
```bash
cd sensor
docker build -t sensor .
```

## 🔹 3. Execute os Containers

**Variáveis de Ambiente**
- BrokerIP: IP do Broker/Setor
- RaftPort: Porta para comunicação do algoritmo Raft
- TcpPort: Porta para comunicação TCP
- MqttPort: Porta para protocolo MQTT
- ID: Identificador do Dispositivo
- Type: Tipo do Sensor (sonar ou radar)
- First: Indica se é o primeiro nó/setor a subir. Necessário para criar o Cluster. (Se primero first = t)
- nodes_Setor: Conjunto de IPs, IDs e Portas dos setores separados por espaço. Modelo: <ID=IP:RaftPort:TcpPort>. Ex: ID<sub>1</sub>=172.xx.xxx.x1:6000:5000 ID<sub>2</sub>=172.xx.xxx.x2:6000:5000
- nodes_Drone: Conjunto de IPs dos brokers MQTT junto com sua porta de uso separados por espaço. Modelo: <ID=IP:MqttPort>. Ex: ID<sub>1</sub>=172.xx.xxx.x1:1883 ID<sub>2</sub>=172.xx.xxx.x2:6000:5000

- **Broker**:
```bash
cd broker
docker run \
		-p <MqttPort>:<MqttPort>/tcp \
		-p <TcpPort>:<TcpPort>/tcp \
		-p <RaftPort>:<RaftPort>/tcp \
		broker ./app <ID> <BrokerIP> \
		<MqttPort> <RaftPort> <TcpPort> \
		<First> <nodes_Setor>
```

- **Cliente**:
```bash
cd cliente
docker run -it cliente ./app <ID> <BrokerIP>:<MqttPort>
```

- **Drone**:
```bash
cd atuador
docker run -it drone ./app <ID> <nodes_Drone>
```

- **Sensor**:
```bash
cd sensor
docker run -it sensor ./app <type> <ID> <BrokerIP>:<MqttPort>
```

## 🔹 4. Execute os Containers (Makefile)
Caso esteja em um sistema Linux, é possível executar os containers facilmente por meio do Makefile.

**Variáveis do Makefile**
- BrokerIP: IP do Broker/Setor
- RaftPort: Porta para comunicação do algoritmo Raft
- TcpPort: Porta para comunicação TCP
- MqttPort: Porta para protocolo MQTT
- ID: Identificador do Dispositivo
- type: Tipo do Sensor (sonar ou radar)
- First: Indica se é o primeiro nó/setor a subir. Necessário para criar o Cluster. (Se primero first = t)
- Nodes: Conjunto de IPs e IDs dos setores separados por espaço. Modelo: <ID=IP>. Ex: "ID<sub>1</sub>=172.xx.xxx.x1 ID<sub>2</sub>=172.xx.xxx.x2" (necessita das aspas)

- **Build**:
```bash
make build
```

- **Broker**:
```bash
make broker MqttPort=<MqttPort> RaftPort=<RaftPort> TcpPort=<TcpPort> First=<First> ID=<ID> BrokerIP=<BrokerIP> Nodes="ID<sub>1</sub>=172.xx.xxx.x1 ID<sub>2</sub>=172.xx.xxx.x2 ..."
```

- **Cliente**:
```bash
make cliente MqttPort=<MqttPort> ID=<ID> BrokerIP=<BrokerIP>
```

- **Drone**:
```bash
make drone MqttPort=<MqttPort> ID=<ID> Nodes="ID<sub>1</sub>=172.xx.xxx.x1 ID<sub>2</sub>=172.xx.xxx.x2 ..."
```

- **Sensor**:
```bash
make sensor MqttPort=<MqttPort> ID=<ID> BrokerIP=<BrokerIP> type=<type>
```

# ⚠️ Limitações

- Interface baseada em terminal
- O algoritmo Raft necessita da maioria dos nós ativos para funcionamento.
- O sistema não utiliza persistência distribuída permanente além dos logs locais do Raft.
