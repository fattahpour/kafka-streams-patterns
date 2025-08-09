#!/bin/bash
mvn -pl rekey-repartition -am package
java -jar rekey-repartition/target/rekey-repartition-1.0.0-SNAPSHOT.jar "$@"
