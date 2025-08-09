#!/bin/bash
mvn -pl agg-window-session -am package
java -jar agg-window-session/target/agg-window-session-1.0.0-SNAPSHOT.jar "$@"
