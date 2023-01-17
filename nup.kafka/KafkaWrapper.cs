using System.Text;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ExampleEvents;
using Newtonsoft.Json;

namespace nup.kafka;

public class KafkaWrapper
{
    private IProducer<string, string>? _producer;
    private string _brokers;
    private string _appName;
    private readonly ProducerOptions _defaultProducerOptions;

    private JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings
    {
        ReferenceLoopHandling = ReferenceLoopHandling.Ignore
    };

    public KafkaWrapper(List<string> brokerList, string appName, ProducerOptions defaultProducerOptions)
    {
        _appName = appName ?? throw new ArgumentNullException(nameof(appName));
        _defaultProducerOptions = defaultProducerOptions ?? throw new ArgumentNullException(nameof(defaultProducerOptions));
        _brokers = string.Join(",", brokerList);
        var config = new ProducerConfig { BootstrapServers = _brokers, Partitioner =Partitioner.Random };
        _producer = new ProducerBuilder<string, string>(config)
            .Build();
    }

    public async Task Send<T>(T ev, ProducerOptions? options = null) where T:class
    {
        string topicName = typeof(T).FullName;
        await CreateTopic(topicName, options);

        try
        {
            // Note: Awaiting the asynchronous produce request below prevents flow of execution
            // from proceeding until the acknowledgement from the broker is received (at the 
            // expense of low throughput).
            Console.WriteLine($"Sending: {JsonConvert.SerializeObject(ev,Formatting.Indented,settings:_jsonSerializerSettings)}");
            var deliveryReport = await _producer.ProduceAsync(
                topicName,
                new Message<string, string>
                {
                    Key = KafkaConsts.Payload, Value = JsonConvert.SerializeObject(ev,settings:_jsonSerializerSettings),
                    Headers = AddHeaders(CreateHeaders(ev))
                });

            Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset}");
        }
        catch (ProduceException<string, string> e)
        {
            Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
        }

        // Since we are producing synchronously, at this point there will be no messages
        // in-flight and no delivery reports waiting to be acknowledged, so there is no
        // need to call producer.Flush before disposing the producer.
    }

    private Dictionary<string, string> CreateHeaders<T>(T ev) where T:class
    {
        return new Dictionary<string, string>
        {
            { KafkaConsts.EventType, typeof(T).FullName },
            { KafkaConsts.CreatedAt, DateTime.UtcNow.ToString() },
            { KafkaConsts.Producer, _appName },
        };
    }

    private static Headers AddHeaders(Dictionary<string, string> headerName)
    {
        var addHeaders = new Headers();
        foreach (var header in headerName.Select(x => new Header(x.Key, Encoding.UTF8.GetBytes(x.Value))))
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

    private async Task CreateTopic(string topicName, ProducerOptions? options)
    {
        options ??= _defaultProducerOptions;
        using (var adminClient =
               new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _brokers }).Build())
        {
            try
            {
                var existingConfig = GetTopicConfig(topicName, adminClient);
                var partitionsCount = existingConfig.Topics.First().Partitions.Count;
                if (existingConfig?.Topics?.Count==1 && partitionsCount!=1 && partitionsCount< options.PartitionCount)
                {
                    await IncreasePartitionCountTo(adminClient, topicName, options.PartitionCount);
                }
                // else
                // {
                //     await adminClient.CreateTopicsAsync(new TopicSpecification[]
                //     {
                //         new TopicSpecification { Name = topicName, NumPartitions = options.PartitionCount }
                //     }, new CreateTopicsOptions { });   
                // }
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }
    }

    private static Metadata GetTopicConfig(string topicName, IAdminClient adminClient)
    {
        return adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(3)); // for some reason this actually creates a topic with 1 partition ???
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