#!/bin/bash
mvn -q -pl $(dirname $0) -am package
java -jar $(dirname $0)/target/fanout-fanin-1.0.0-SNAPSHOT.jar
