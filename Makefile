.PHONY: up down bootstrap build test run

up:            ## start local Redpanda (broker + schema registry)
	docker-compose up -d

down:
	docker-compose down

bootstrap:     ## create topics + register schemas
	./scripts/bootstrap-local-topics.sh

build:
	mvn clean verify

test:
	mvn test

run:
	 mvn exec:java -Dexec.mainClass="com.healthcare.rxvigilance.AdherenceJob" \
              -Dexec.args="--kafka.brokers=localhost:9092 \
                           --schema.registry.url=http://localhost:8081 \
                           --checkpoint.dir=file:///tmp/rx-vigilance-checkpoints"