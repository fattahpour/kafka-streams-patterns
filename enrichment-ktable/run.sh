#!/bin/bash
mvn -q -pl $(dirname $0) -am package
java -jar $(dirname $0)/target/enrichment-ktable-1.0.0-SNAPSHOT.jar
