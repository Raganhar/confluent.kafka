# confluent.kafka

- [x] Send message
- [x] Receive message
- [x] Receive message and deserialize to correct type
- [ ] Competing consumers
- [ ] define partition key such as primary key / guid so entities always gets processed by the same partition
- [ ] FIFO consumption by partition 1 with 1 level of parallelism to guarantee sequence on partition
- [ ] FIFO consumption by partition on aggregate
- [ ] Idempotent consumption
- [ ] Deadletter queue with message attribute describing failure reason
- [ ] Fail successive events on same aggregate if earlier events on same entity ID has previously failed (so we dont process update events, if the create event failed)
- [ ] Provide way to "requeue" events from deadletter queue
- [ ] Kafka Transactions
- [ ] Kafka connect POC for shoveling events from Kafka to SNS/SQS & from SNS/SQS to Kafka
