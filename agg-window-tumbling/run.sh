#!/bin/bash
mvn -pl agg-window-tumbling -am package
java -jar agg-window-tumbling/target/agg-window-tumbling-1.0.0-SNAPSHOT.jar "$@"
