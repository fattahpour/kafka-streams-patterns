#!/bin/bash
mvn -pl late-early-data -am package
java -jar late-early-data/target/late-early-data-1.0.0-SNAPSHOT.jar "$@"
