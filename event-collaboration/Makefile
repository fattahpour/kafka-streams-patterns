MODULE=event-collaboration

.PHONY: build test run

build:
	mvn -pl $(MODULE) -am clean package

test:
	mvn -pl $(MODULE) -am test

run: build
	mvn -pl $(MODULE) -am -DskipTests exec:java -Dexec.mainClass=com.fattahpour.kstreamspatterns.eventcollaboration.EventCollaborationApp
