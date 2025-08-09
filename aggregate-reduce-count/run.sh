#!/bin/bash
mvn -pl aggregate-reduce-count -am package
java -jar aggregate-reduce-count/target/aggregate-reduce-count-1.0.0-SNAPSHOT.jar "$@"
