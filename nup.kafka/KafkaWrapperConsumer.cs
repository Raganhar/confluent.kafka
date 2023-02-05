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
    private readonly EventProcesser _eventProcesser;
    private ILogger _logger;
    private KafkaOptions _consumerOptions;

    public KafkaWrapperConsumer(KafkaOptions consumerOptions, EventProcesser eventProcesser,
        string consumerIdentifier = "default")
    {
        _consumerOptions = consumerOptions;
        if (consumerOptions == null) throw new ArgumentNullException(nameof(consumerOptions));
        _logger = Log.ForContext("Executor", this.GetType().Name);
        _consumerIdentifier = consumerIdentifier;
        _appName = consumerOptions.AppName ?? throw new ArgumentNullException(nameof(consumerOptions.AppName));
        _brokers = string.Join(",", consumerOptions.Brokers);
        _eventProcesser = eventProcesser;
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

    public Task Consume(CancellationToken cancellationToken, Action<ConsumeResult<Ignore, string>> handler,
        string topic)
    {
        try
        {
            var task = Task.Factory.StartNew(() => RunListener(_brokers, topic, cancellationToken, handler),
                cancellationToken, TaskCreationOptions.LongRunning,TaskScheduler.Default);
            task.ConfigureAwait(false);
            return task;
        }
        catch (Exception e)
        {
            _logger.Error(e, "failed to register listener");
            return Task.CompletedTask;
        }
        // _handlers.AddOrUpdate(topic, null as string, (s, s1) => null);
    }

    public void Consume<T>(CancellationToken cancellationToken, Action<T> handler)
    {
        try
        {
            var topic = typeof(T).FullName;
            // if (_handlers.ContainsKey(topic))
            // {
            //     throw new ArgumentException($"Handler for topic {topic} is already registered");
            // }

            ThreadPool.QueueUserWorkItem(state => RunListener(_brokers, topic, cancellationToken, handler),
                "ThreadPool");
            // _handlers.AddOrUpdate(topic, null as string, (s, s1) => null);
        }
        catch (Exception e)
        {
            _logger.Error(e, "failed to register listener");
        }
    }

    private void RunListener<T>(string brokerList, string topics, CancellationToken cancellationToken,
        Action<T> handler)
    {
        var config = ConsumerConfig<T>(brokerList);

        // Note: If a key or value deserializer is not set (as is the case below), the 
        // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
        // will be used automatically (where available). The default deserializer for string
        // is UTF8. The default deserializer for Ignore returns null for all input data
        // (including non-null data).
        using (var consumer = CreateConsumer<T>(config))
        {
            try
            {
                consumer.Subscribe(topics);
                _logger.Information("Registered kafka consumer to {topic}", topics);
            }
            catch (Exception e)
            {
                _logger.Error(e, "Failed to subscribe to topic.");
                throw;
            }

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        if (consumeResult.IsPartitionEOF)
                        {
                            _logger.Information(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                            continue;
                        }

                        _eventProcesser.ProcessMessage(handler, consumeResult, () =>
                        {
                            _logger.Information("Saving offset");
                            consumer.StoreOffset(consumeResult);
                            _logger.Information("Saved offset");
                        }, _persistence, _consumerIdentifier);
                    }
                    catch (ConsumeException e)
                    {
                        _logger.Information($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.Information("Closing consumer.");
                consumer.Close();
            }
        }
    }

    private IConsumer<Ignore, string> CreateConsumer<T>(ConsumerConfig config)
    {
        return new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler((_, e) => _logger.Information($"Error: {e.Reason}"))
            .SetStatisticsHandler((_, json) => _logger.Information($"Statistics: {json}"))
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                // Since a cooperative assignor (CooperativeSticky) has been configured, the
                // partition assignment is incremental (adds partitions to any existing assignment).
                _logger.Information(
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
                _logger.Information(
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
                _logger.Information($"Partitions were lost: [{string.Join(", ", partitions)}]");
            })
            .Build();
    }

    private ConsumerConfig ConsumerConfig<T>(string brokerList)
    {
        var clientConfig = KafkaUtils.InitConfig(_consumerOptions);
        var config = new ConsumerConfig(clientConfig);
        config.BootstrapServers = brokerList;
        config.GroupId = _appName;
        config.EnableAutoOffsetStore = false; //if true, it will commit offset BEFORE data is returned in consume()
        config.EnableAutoCommit = true;
        config.StatisticsIntervalMs = 5000;
        config.SessionTimeoutMs = 6000;
        config.AutoOffsetReset = AutoOffsetReset.Earliest;
        config.AutoCommitIntervalMs = 1000;
        config.EnablePartitionEof =
            true; // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
        // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
        config.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
        config.TopicBlacklist = "docker-connect.*,_.*";
        return config;
    }
}