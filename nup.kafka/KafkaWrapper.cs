using System.Text;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ExampleEvents;
using Newtonsoft.Json;
using nup.kafka.Models;
using Serilog;

namespace nup.kafka;

public class KafkaWrapper
{
    private IProducer<string, string>? _producer;
    private string _brokers;
    private string _appName;
    private readonly KafkaOptions _defaultKafkaOptions;

    private JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings
    {
        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
    };

    public KafkaWrapper(string appName, KafkaOptions defaultKafkaOptions)
    {
        _appName = appName ?? throw new ArgumentNullException(nameof(appName));
        _defaultKafkaOptions =
            defaultKafkaOptions ?? throw new ArgumentNullException(nameof(defaultKafkaOptions));
        _brokers = string.Join(",", defaultKafkaOptions.Brokers);
        var config = KafkaUtils.InitConfig(defaultKafkaOptions);
        _producer = new ProducerBuilder<string, string>(config)
            .Build();
    }

    public async Task Send<T>(T ev, string entityKey = null, KafkaOptions? options = null) where T : class
    {
        var topic = typeof(T).FullName;
        var payload = JsonConvert.SerializeObject(ev, settings: _jsonSerializerSettings);
        await Send(payload, topic, topic, OriginatingPlatform.Kafka, entityKey, options);
    }

    public async Task Send(string payload, string topic, string eventType, OriginatingPlatform platform,
        string entityKey = null, KafkaOptions? options = null)
    {
        await CreateTopic(topic, options);

        try
        {
            // Note: Awaiting the asynchronous produce request below prevents flow of execution
            // from proceeding until the acknowledgement from the broker is received (at the 
            // expense of low throughput).
            Log.Information("Sending: {entityKey}", entityKey,
                payload, Formatting.Indented);
            var deliveryReport = await _producer.ProduceAsync(
                topic,
                new Message<string, string>
                {
                    Key = entityKey,
                    Value = payload,
                    Headers = AddHeaders(CreateHeaders(eventType, entityKey, topic, platform))
                });

            Log.Information("delivered to: {TopicPartitionOffset} with entityKey: {entityKey}",
                deliveryReport.TopicPartitionOffset, entityKey);
        }
        catch (ProduceException<string, string> e)
        {
            Log.Information($"failed to deliver message: {e.Message} [{e.Error.Code}]");
        }
        catch (Exception e)
        {
            Log.Error("Unhandled error when sending message to kafka {@exception}", e);
        }

        // Since we are producing synchronously, at this point there will be no messages
        // Since we are producing synchronously, at this point there will be no messages
        // in-flight and no delivery reports waiting to be acknowledged, so there is no
        // need to call producer.Flush before disposing the producer.
    }

    private Dictionary<string, string> CreateHeaders(string eventType, string entityKey, string topic,
        OriginatingPlatform originatingPlatform)
    {
        return new Dictionary<string, string>
        {
            { KafkaConsts.Topic, topic },
            { KafkaConsts.EventType, eventType },
            { KafkaConsts.CreatedAt, DateTime.UtcNow.ToCorrectStringFormat() },
            { KafkaConsts.Producer, _appName },
            { KafkaConsts.PartitionKey, entityKey },
            { KafkaConsts.OriginatedAt, originatingPlatform.ToString() },
        };
    }

    public static Headers AddHeaders(Dictionary<string, string> headerName)
    {
        var addHeaders = new Headers();
        foreach (var header in headerName.Where(x => !string.IsNullOrWhiteSpace(x.Value))
                     .Select(x => new Header(x.Key, Encoding.UTF8.GetBytes(x.Value))))
        {
            addHeaders.Add(header);
        }

        return addHeaders;
    }

    private static Dictionary<string, string> GetHeaders(ConsumeResult<Ignore, string> consumeResult)
    {
        return
            consumeResult.Message.Headers?.ToDictionary(x => x.Key, x => Encoding.UTF8.GetString(x.GetValueBytes())) ??
            new Dictionary<string, string>();
    }

    public async Task CreateTopic(string topicName, KafkaOptions? options)
    {
        options ??= _defaultKafkaOptions;
        using (var adminClient =
               new AdminClientBuilder(KafkaUtils.InitConfig(options)).Build())
        {
            try
            {
                var existingConfig = GetTopicConfig(topicName, adminClient);
                var partitionsCount = existingConfig.Topics.First().Partitions.Count;

                var unknownTopic = existingConfig.Topics.Any(x => x.Error != null);
                if (unknownTopic)
                {
                    {
                        await adminClient.CreateTopicsAsync(new TopicSpecification[]
                        {
                            new TopicSpecification { Name = topicName, NumPartitions = options.PartitionCount }
                        }, new CreateTopicsOptions { });
                    }
                }
                else
                {
                    if (existingConfig?.Topics?.Count == 1 && partitionsCount != 1 &&
                        partitionsCount < options.PartitionCount)
                    {
                        await IncreasePartitionCountTo(adminClient, topicName, options.PartitionCount);
                    }
                }
            }
            catch (CreateTopicsException e)
            {
                Log.Information($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
            catch (Exception e)
            {
                Log.Information($"An error occured {e.Message}");
            }
        }
    }

    private static Metadata GetTopicConfig(string topicName, IAdminClient adminClient)
    {
        return
            adminClient.GetMetadata(topicName,
                TimeSpan.FromSeconds(3)); // for some reason this actually creates a topic with 1 partition ???
    }


    private static async Task IncreasePartitionCountTo(IAdminClient adminClient, string topicName, int count)
    {
        await adminClient.CreatePartitionsAsync(new List<PartitionsSpecification>
        {
            new PartitionsSpecification
            {
                Topic = topicName,
                IncreaseTo = count,
            }
        });
    }
}