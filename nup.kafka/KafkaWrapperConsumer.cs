using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ExampleEvents;
using Newtonsoft.Json;

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

    public KafkaWrapperConsumer(List<string> brokerList, string appName)
    {
        _appName = appName ?? throw new ArgumentNullException(nameof(appName));
        _brokers = string.Join(",", brokerList);
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

    private static void RunListener<T>(string brokerList, string topics, CancellationToken cancellationToken, Action<T> handler)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokerList,
            GroupId = "csharp-consumer",
            EnableAutoOffsetStore = true,
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
                   .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
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
                            Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        var headers = GetHeaders(consumeResult);

                        var eventObj = JsonConvert.DeserializeObject<T>(consumeResult.Message.Value);
                        Console.WriteLine(
                            $"Received message at {consumeResult.TopicPartitionOffset}: {JsonConvert.SerializeObject(eventObj)}");
                        handler(eventObj);
                        Console.WriteLine(
                            $"Handled message at {consumeResult.TopicPartitionOffset}: {JsonConvert.SerializeObject(eventObj)}");
                        try
                        {
                            // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                            // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                            // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                            // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                            consumer.StoreOffset(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }
        }
    }
    private static Dictionary<string, string> GetHeaders(ConsumeResult<Ignore, string> consumeResult)
    {
        return
            consumeResult.Message.Headers?.ToDictionary(x => x.Key, x => Encoding.UTF8.GetString(x.GetValueBytes())) ??
            new Dictionary<string, string>();
    }
}