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
- [ ] Kafka Transactions
- [x] Kafka connect POC for shoveling events from Kafka to SNS/SQS & from SNS/SQS to Kafka
- [ ] Figure out if a unique event ID has to be added the header, indicating the uniqueness of the payload. This can be used to later identify the same event payload if it is requeued on another partition after rebalancing of partitions (adding more partitions)
- [ ] Implement claims check

## support processes
- [ ] Provide way to "requeue" events from deadletter queue
- [ ] Provide a way to reset offsets, allowing to reprocess previous records
- [ ] Provide a way to set the offset, allowing to skip poisoned records
- [ ] Figure out how to mark messages as "will never be processed" so that they wont block further "aggregrate" processing

## random thoughts
- [ ] look into allowig a level of parallelism when consuming events, fx scheduling multiple consumers on the same host for the sam topic. Enabling 2 hosts to be assigned 10 partitions - 5 each. To consume 5 partitions in parallel per host, instad of only processing 1 partition at a time per host
- [ ] Look into doing in app parallelizatiom of individual paritition, by doing an extra layer of caching the partition key, allowing 2 partitions to be processed in parallel by fx 10 workers in parallel. This would obviously require some sort of batch processing consuming batches at a time and batch committing offset


dotnet ef migrations add InitialCreate
