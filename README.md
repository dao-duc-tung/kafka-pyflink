# Kafka - PyFlink

Ref: https://apache.googlesource.com/flink-playgrounds/+/HEAD/pyflink-walkthrough

## Quickstart

```bash
# run compose
docker-compose build
docker-compose up -d

# check topic
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic payment_msg

# submit flink job
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink-walkthrough/payment_msg_proccessing.py -d

# go to flink UI: http://localhost:8081

# check topic
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic payment_msg_3

# tear down
docker-compose down
```
