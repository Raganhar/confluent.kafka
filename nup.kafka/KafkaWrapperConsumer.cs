using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ExampleEvents;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using nup.kafka.DatabaseStuff;
using Serilog;
using Serilog.Context;

namespace nup.kafka;

public class KafkaWrapperConsumer
{
    private string _brokers;
    private string _appName;
    private ConcurrentDictionary<string, string> _handlers = new ConcurrentDictionary<string, string>();

    private JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings
    {
        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
    };

    private string _consumerIdentifier;
    private IDaoLayer _persistence;

    public KafkaWrapperConsumer(List<string> brokerList, string appName,
        string consumerIdentifier = "default")
    {
        _consumerIdentifier = consumerIdentifier;
        _appName = appName ?? throw new ArgumentNullException(nameof(appName));
        _brokers = string.Join(",", brokerList);
    }

    public KafkaWrapperConsumer WithDatabase(IDaoLayer dao)
    {
        _persistence = dao;
        return this;
    }
    public KafkaWrapperConsumer WithDatabase(string connectionString)
    {
        DbContextOptionsBuilder<KafkaMysqlDbContext> optionsBuilder =
            new DbContextOptionsBuilder<KafkaMysqlDbContext>().UseMySql(connectionString,
                ServerVersion.AutoDetect(connectionString), mysqlOptions => mysqlOptions.UseNetTopologySuite());
        var db = new KafkaMysqlDbContext(optionsBuilder.Options); //yolo singleton DB context
        _persistence = new DaoLayer(db);
        return this;
    }

    public void Consume<T>(CancellationToken cancellationToken, Action<T> handler)
    {
        var topic = typeof(SampleEvent1).FullName;
        if (_handlers.ContainsKey(topic))
        {
            throw new ArgumentException($"Handler for topic {topic} is already registered");
        }

        ThreadPool.QueueUserWorkItem(state => RunListener(_brokers, topic, cancellationToken, handler), "ThreadPool");
        _handlers.AddOrUpdate(topic, null as string, (s, s1) => null);
    }

    private void RunListener<T>(string brokerList, string topics, CancellationToken cancellationToken,
        Action<T> handler)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = _appName,
            EnableAutoOffsetStore = false,//if true, it will commit offset BEFORE data is returned in consume()
            EnableAutoCommit = true,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AutoCommitIntervalMs = 1000,
            EnablePartitionEof = true,
            // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
            // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
        };

        // Note: If a key or value deserializer is not set (as is the case below), the 
        // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
        // will be used automatically (where available). The default deserializer for string
        // is UTF8. The default deserializer for Ignore returns null for all input data
        // (including non-null data).
        using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                   .SetErrorHandler((_, e) => Log.Information($"Error: {e.Reason}"))
                   .SetStatisticsHandler((_, json) => Log.Information($"Statistics: {json}"))
                   .SetPartitionsAssignedHandler((c, partitions) =>
                   {
                       // Since a cooperative assignor (CooperativeSticky) has been configured, the
                       // partition assignment is incremental (adds partitions to any existing assignment).
                       Log.Information(
                           "Partitions incrementally assigned: [" +
                           string.Join(',', partitions.Select(p => p.Partition.Value)) +
                           "], all: [" +
                           string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                           "]");

                       // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                       // to assign to, e.g.:
                       // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                   })
                   .SetPartitionsRevokedHandler((c, partitions) =>
                   {
                       // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                       // assignment is incremental (may remove only some partitions of the current assignment).
                       var remaining = c.Assignment.Where(atp =>
                           partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                       Log.Information(
                           "Partitions incrementally revoked: [" +
                           string.Join(',', partitions.Select(p => p.Partition.Value)) +
                           "], remaining: [" +
                           string.Join(',', remaining.Select(p => p.Partition.Value)) +
                           "]");
                   })
                   .SetPartitionsLostHandler((c, partitions) =>
                   {
                       // The lost partitions handler is called when the consumer detects that it has lost ownership
                       // of its assignment (fallen out of the group).
                       Log.Information($"Partitions were lost: [{string.Join(", ", partitions)}]");
                   })
                   .Build())
        {
            consumer.Subscribe(topics);

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        if (consumeResult.IsPartitionEOF)
                        {
                            Log.Information(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                            continue;
                        }
                        ProcessMessage(handler, consumeResult, ()=> consumer.StoreOffset(consumeResult), _persistence);
                    }
                    catch (ConsumeException e)
                    {
                        Log.Information($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Log.Information("Closing consumer.");
                consumer.Close();
            }
        }
    }

    private void ProcessMessage<T>(Action<T> handler, ConsumeResult<Ignore, string> consumeResult, Action storeOffset, IDaoLayer daoLayer)
    {
        using (LogContext.PushProperty("TopicPartitionOffset", consumeResult.TopicPartitionOffset))
        {
            var recievedAtUtc = DateTime.UtcNow;
            var headers = GetHeaders(consumeResult);

            var partitionKey = headers.ContainsKey(KafkaConsts.PartitionKey)
                ? headers[KafkaConsts.PartitionKey]
                : null;
            var previouslyProcessedMessage =
                daoLayer.Get(consumeResult.TopicPartitionOffset, partitionKey);
            if (previouslyProcessedMessage?.ProcessedSuccefully == true)
            {
                Log.Information(
                    "Received message on topicPartition: {TopicPartitionOffset} which was already successfully processed",
                    consumeResult.TopicPartitionOffset);
                storeOffset();
                Log.Information("Stored offset (ignored the message)");
                return;
            }

            var kafkaMessage = new KafkaMessage
            {
                Partition = consumeResult.Partition.Value,
                OffSet = consumeResult.Offset.Value,
                FinishedProcessingAtUtc = DateTime.UtcNow,
                RecievedCreatedAtUtc = recievedAtUtc,
                PartitionKey = partitionKey,
                ProcessedSuccefully = true,
                Topic = consumeResult.Topic
            };

            if (partitionKey != null)
            {
                var previousAggregateEntityFailed =
                    daoLayer.DidPreviousRelatedEntityFail(consumeResult.TopicPartitionOffset, partitionKey);
                if (previousAggregateEntityFailed)
                {
                    Log.Information(
                        "Previous entity message failed to be processed, will not process this in order to guarantee order of execution for topicPartition: {TopicPartitionOffset}",
                        consumeResult.TopicPartitionOffset);
                    storeOffset();
                    kafkaMessage.ProcessedSuccefully = false;
                    kafkaMessage.ReasonText = "Previous entity message failed to be processed";
                    daoLayer.AddEvent(kafkaMessage);

                    Log.Information("Stored offset (ignored the message)");
                    return;
                }
            }

            try
            {
                var eventObj = JsonConvert.DeserializeObject<T>(consumeResult.Message.Value);
                Log.Information(
                    $"{_consumerIdentifier} Received message at {consumeResult.TopicPartitionOffset}: {JsonConvert.SerializeObject(eventObj)}");
                handler(eventObj);
                Log.Information(
                    $"Handled message at {consumeResult.TopicPartitionOffset}: {JsonConvert.SerializeObject(eventObj)}");
            }
            catch (Exception e)
            {
                Log.Error("Failed to process event at {TopicPartitionOffset}");
                SaveFailedMessage(kafkaMessage, e);
                throw;
            }

            try
            {
                // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                storeOffset();
                daoLayer.AddEvent(kafkaMessage);
            }
            catch (KafkaException e)
            {
                Log.Information($"Store Offset error: {e.Error.Reason}");
                SaveFailedMessage(kafkaMessage, e);
            }
        }
    }

    private void SaveFailedMessage(KafkaMessage kafkaMessage, Exception e)
    {
        kafkaMessage.ProcessedSuccefully = false;
        kafkaMessage.ReasonText = e.Message;
        _persistence.AddEvent(kafkaMessage);
    }

    private static Dictionary<string, string> GetHeaders(ConsumeResult<Ignore, string> consumeResult)
    {
        return
            consumeResult.Message.Headers?.ToDictionary(x => x.Key, x => Encoding.UTF8.GetString(x.GetValueBytes())) ??
            new Dictionary<string, string>();
    }
}