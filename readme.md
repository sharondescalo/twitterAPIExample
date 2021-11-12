# TwitterAPI-Kafka Example

## Run Kafka Cluster and application
```
docker-compose up -d
```
## Topic Configuration

```
docker exec kafka1 /opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic test-topic --partitions 3 --replication-factor 3
```


