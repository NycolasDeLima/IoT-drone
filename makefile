N ?= localhost
First ?= t

Nodes ?= localhost

RaftPort ?= 6000
TcpPort ?= 5001

ID ?= 1

type ?= radar

BrokerIP ?= localhost
MqttPort ?= 1883

NODES_FORMATADOS_DRONE = $(foreach node,$(Nodes),$(node):$(MqttPort))

NODES_FORMATADOS_SERVER = $(foreach node,$(Nodes),$(node):$(RaftPort):$(TcpPort))

.PHONY: broker sensor atuador cliente drone

build:
	cd sensor && docker build -t sensor .
	cd cliente && docker build -t cliente .
	cd broker && docker build -t broker .
	cd drone && docker build -t drone .

setor:

sensor:
	cd sensor && for i in $$(seq 1 $(N)); do \
		docker run -d sensor ./app $(type) $$i $(BrokerIP):$(MqttPort); \
	done

drone:

	cd drone && docker run -it drone ./app $(ID) $(NODES_FORMATADOS_DRONE)


cliente:

	cd cliente && docker run -it cliente ./app $(ID) $(BrokerIP):$(MqttPort); \


broker:
	cd broker && docker run \
		-p $(MqttPort):$(MqttPort)/tcp \
		-p $(TcpPort):$(TcpPort)/tcp \
		-p $(RaftPort):$(RaftPort)/tcp \
		broker ./app $(ID) $(BrokerIP) \
		$(MqttPort) $(RaftPort) $(TcpPort) \
		$(First) $(NODES_FORMATADOS_SERVER)

