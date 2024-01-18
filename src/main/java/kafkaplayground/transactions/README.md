# Setup
1. Create 3 topics: `purchase, stock, parcels`
```
./kafka-topics.sh --create --topic purchase --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2
./kafka-topics.sh --create --topic shipments --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2
./kafka-topics.sh --create --topic invoices --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2
```

# Idempotent producer

### Produce purchase events
```
./kafka-console-producer.sh --topic purchase --bootstrap-server localhost:9092
{"purchaseId": "purch1", "productId":"p1","userId":"u1","quantity":2,"userFullName":"Daniel Faderski","totalPrice":"20$"}
```

### Consume shipment and invoice events
```
./kafka-console-consumer.sh --topic invoices --bootstrap-server localhost:9092 --from-beginning
{"userId":"u1","productId":"p1","quantity":2,"totalPrice":"20$"}
```

```
./kafka-console-consumer.sh --topic shipments --bootstrap-server localhost:9092 --from-beginning
{"shipmentId":"de2fe364-c619-4cbf-9a3c-b71745583fad","productId":"p1","recipient":"Daniel Faderski","address":"SomeStreet 12/18, 00-006 Warsaw","quantity":2}
```