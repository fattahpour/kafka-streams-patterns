# Aggregate Reduce Count Pattern

Shows the difference between `aggregate`, `reduce` and `count` when grouping records.

## Scenario

Compare aggregation behaviors when summarizing metrics, such as tracking total sales, maximum order value, and count per product.

Topology:
```
input -> groupByKey -> aggregate(sum) -> sum-topic
                      \-> reduce(max) -> max-topic
                      \-> count      -> count-topic
```

## How to run

```bash
mvn -pl aggregate-reduce-count -am clean package
java -jar aggregate-reduce-count/target/aggregate-reduce-count-1.0.0-SNAPSHOT.jar \
  -Dbootstrap.servers=localhost:9092 \
  -Dapplication.id=aggregate-reduce-count \
  -Dinput.topic=input-arc \
  -Dsum.topic=sum-arc \
  -Dmax.topic=max-arc \
  -Dcount.topic=count-arc
```
