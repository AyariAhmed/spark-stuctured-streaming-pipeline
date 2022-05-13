### Spark structured streaming && kafka
Spark Structured Streaming is the new Spark stream processing approach, available from Spark 2.0 and stable from Spark 2.2. Spark Structured Streaming processing engine is built on the Spark SQL engine and both share the same high-level API.

- used versions : spark-2.4.0 and kafka_2.12-2.2.0
```bash
# After downloading the appropriate zipped filed

tar xzf spark-2.4.0-bin-hadoop2.7.tgz
ln -sf spark-2.4.0-bin-hadoop2.7 spark

tar xzf spark-2.4.0-bin-hadoop2.7.tgz
ln -sf spark-2.4.0-bin-hadoop2.7 spark

```
- To set spark output to WARN
```bash
cp spark/conf/log4j.properties.template spark/conf/log4j.properties
sed -i -e 's/log4j.rootCategory=INFO/log4j.rootCategory=WARN/g' spark/conf/log4j.properties
```
- start zookeeper server and kafka server,then create two kafka topics `taxirides` and `taxifares`
```bash
kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties
kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties
kafka/bin/kafka-topics.sh \
  --create --zookeeper localhost:2181 --replication-factor 1 \
  --partitions 1 --topic taxirides
kafka/bin/kafka-topics.sh \
  --create --zookeeper localhost:2181 --replication-factor 1 \
  --partitions 1 --topic taxifares
```
- ingestions of the two datasets: pushed into a kafka producer which then will be consumed with a kafka consumer
```bash
( zcat data/nycTaxiFares.gz \
  | split -l 10000 --filter="kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 --topic taxifares; sleep 0.5" \
  > /dev/null ) &

( zcat data/nycTaxiRides.gz \
  | split -l 10000 --filter="kafka/bin/kafka-console-producer.sh \
    --broker-list localhost:9092 --topic taxirides; sleep 0.2" \
  > /dev/null ) &

```
> Notice the difference in sleep time: since the rides dataset is larger than the fares dataset, they mustn't be consumed at the same rate otherwide stream-stream joins won't be possible. (10000 messages/event)
- To verify that the data was successfully registered on the specified topics
```bash
kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic taxirides --from-beginning
kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic taxifares --from-beginning
```
- Run the pipeline through: 
```bash
spark/bin/spark-submit   --master local --driver-memory 4g   --num-executors 2 --executor-memory 4g   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0  sstreaming-spark-out.py 
```
> (Data streams should be restarted when **Batch 0** appears in the console)
