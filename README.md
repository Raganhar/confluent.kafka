# confluent.kafka

- [x] Send message
- [x] Receive message
- [x] Receive message and deserialize to correct type
- [x] Competing consumers
- [x] define partition key such as primary key / guid so entities always gets processed by the same partition
- [x] FIFO consumption by partition 1 with 1 level of parallelism to guarantee sequence on partition
- [ ] FIFO consumption by partition on aggregate
- [X] Idempotent consumption
- [ ] Deadletter queue with message attribute describing failure reason
- [x] Fail successive events on same aggregate if earlier events on same entity ID has previously failed (so we dont process update events, if the create event failed)
- [ ] Provide way to "requeue" events from deadletter queue
- [ ] Kafka Transactions
- [ ] Kafka connect POC for shoveling events from Kafka to SNS/SQS & from SNS/SQS to Kafka
- [ ] Figure out how to mark messages as "will never be processed" so that they wont block further "aggregrate" processing
- [ ] Figure out if a unique event ID has to be added the header, indicating the uniqueness of the payload. This can be used to later identify the same event payload if it is requeued on another partition after rebalancing of partitions (adding more partitions)

dotnet ef migrations add InitialCreate