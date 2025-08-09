#!/bin/bash
mvn -pl agg-window-hopping -am package
java -jar agg-window-hopping/target/agg-window-hopping-1.0.0-SNAPSHOT.jar "$@"
