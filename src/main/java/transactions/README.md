# Setup
1. Create 3 topics: `purchase, stock, parcels`
```
./kafka-topics.sh --create --topic purchase --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2
./kafka-topics.sh --create --topic stock --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2
./kafka-topics.sh --create --topic parcels --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2
```

# Idempotent producer

### Produce purchase events
```
./kafka-console-producer.sh --topic purchase --bootstrap-server localhost:9092
>{"purchaseId": "3", "productId":"id1","userId":"user1","quantity":2}
```

### Consume parcel and stock events
```
./kafka-console-consumer.sh --topic parcels --bootstrap-server localhost:9092 --from-beginning                                                                                                ✘ 130
{"parcelId":"07d511d9-8a76-440f-8dfa-a739c931c9ca","productId":"id1","userId":"user1","quantity":2}
```

```
./kafka-console-consumer.sh --topic stock --bootstrap-server localhost:9092 --from-beginning                                                                                                  ✘ 130
{"purchaseId":"3","productId":"id1","diff":-2}
```