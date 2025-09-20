MODULE=idempotent-writer-reader

.PHONY: build test run

build:
mvn -pl $(MODULE) -am clean package

test:
mvn -pl $(MODULE) -am test

run: build
mvn -pl $(MODULE) -am -DskipTests exec:java -Dexec.mainClass=io.zyvoxal.kstreamspatterns.idempotentwriterreader.IdempotentWriterReaderApp
