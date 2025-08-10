# join-ktable-ktable

Join two tables and emit the joined values.

```
table-a ---\
           +--> join --> joined
 table-b --/
```

## How to run

```bash
mvn -pl join-ktable-ktable -am clean package
java -jar join-ktable-ktable/target/join-ktable-ktable-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=join-ktable-ktable \
  -Dleft.table.topic=left-table \
  -Dright.table.topic=right-table \
  -Doutput.topic=joined-table
```

Produce records on `left-table` and `right-table` and observe joined values on `joined-table`.
