N ?= 1
ip ?= localhost
types ?= bpm
typea ?= vmi
udp ?= 8080
tcp ?= 1883

.PHONY: broker sensor atuador cliente compose_sensor compose_atuador compose_cliente compose_broker

build:
	cd sensor && docker build -t sensor .
	cd cliente && docker build -t cliente .
	cd broker && docker build -t broker .

compose_sensor:
	for i in $$(seq 1 $(N)); do \
		docker compose run -d sensor ./app $(types) $$i $(ip):$(udp); \
	done

compose_atuador:
	for i in $$(seq 1 $(N)); do \
		docker compose run -d atuador ./app $(typea) $$i $(ip):$(tcp); \
	done

compose_cliente:
	for i in $$(seq 1 $(N)); do \
		docker compose run cliente ./app $$i; $(ip):$(tcp)\
	done

compose_broker:
	docker compose up broker

sensor:
	cd sensor && for i in $$(seq 1 $(N)); do \
		docker run -d sensor ./app $(types) $$i $(ip):$(udp); \
	done

atuador:
	cd atuador && for i in $$(seq 1 $(N)); do \
		docker run -d atuador ./app $(typea) $$i $(ip):$(tcp); \
	done

cliente:
	cd cliente && for i in $$(seq 1 $(N)); do \
		docker run -d cliente ./app $$i $(ip):$(tcp); \
	done

broker:
	cd broker && docker run -p $(udp):$(udp)/udp -p $(tcp):$(tcp)/tcp broker ./app $(udp) $(tcp)