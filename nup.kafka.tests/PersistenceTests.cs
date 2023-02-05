using Confluent.Kafka;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using ExampleEvents;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using nup.kafka.DatabaseStuff;

namespace nup.kafka.tests;

public class PersistenceTests
{
    private DaoLayer _daoLayer;

    public MySqlTestcontainer _database = new TestcontainersBuilder<MySqlTestcontainer>()
        .WithDatabase(new MySqlTestcontainerConfiguration
        {
            Database = "KafkaMysql",
            Password = "root",
            Username = "root",
        }).Build();

    private KafkaMysqlDbContext _db;

    [SetUp]
    public void Setup()
    {
        _database.StartAsync().Wait();
        DbContextOptionsBuilder<KafkaMysqlDbContext> optionsBuilder =
            new DbContextOptionsBuilder<KafkaMysqlDbContext>().UseMySql(_database.ConnectionString,
                ServerVersion.AutoDetect(_database.ConnectionString),
                mysqlOptions => mysqlOptions.UseNetTopologySuite());
        _db = new KafkaMysqlDbContext(optionsBuilder.Options);
        _db.Database.Migrate();
        _daoLayer = new DaoLayer(_db);
    }

    [TearDown]
    public void TearDown()
    {
        
        _database.StopAsync().Wait();
        _database.CleanUpAsync().Wait();
    }

    [Test]
    public void SaveEvent()
    {
        _db.KafkaEvents.Any().Should().BeFalse();
        var kafkaMessage = new KafkaMessage
        {
            Partition = 3,
            Topic = "asdasd",
            OffSet = long.MaxValue - 1,
            ProcessedSuccefully = true,
            ReasonText = "asds",
            PartitionKey = "fsdfdsf",
            FinishedProcessingAtUtc = DateTime.UtcNow,
            RecievedCreatedAtUtc = DateTime.UtcNow,
        };
        _daoLayer.AddEvent(kafkaMessage);

        var message = _db.KafkaEvents.First(x => x.Id == kafkaMessage.Id);
    }

    [Test]
    public void ReturnTrueIfAlreadyProcessed()
    {
        _db.KafkaEvents.Any().Should().BeFalse();
        var kafkaMessage = new KafkaMessage
        {
            Partition = 3,
            Topic = "asdasd",
            OffSet = long.MaxValue - 1,
            ProcessedSuccefully = true,
            ReasonText = "asds",
            PartitionKey = "fsdfdsf",
            FinishedProcessingAtUtc = DateTime.UtcNow,
            RecievedCreatedAtUtc = DateTime.UtcNow,
        };
        _daoLayer.AddEvent(kafkaMessage);

        var message =
            _daoLayer.Get(
                new TopicPartitionOffset(kafkaMessage.Topic, new Partition(kafkaMessage.Partition),
                    kafkaMessage.OffSet));
        message.Should().NotBeNull();
    }

    [Test]
    public void ReturnTrueIfPreviousEntityMessageFailedToProcess()
    {
        _db.KafkaEvents.Any().Should().BeFalse();
        var kafkaMessage = new KafkaMessage
        {
            Partition = 3,
            Topic = "asdasd",
            OffSet = long.MaxValue - 2,
            ProcessedSuccefully = false,
            ReasonText = "asds",
            PartitionKey = "fsdfdsf",
            FinishedProcessingAtUtc = DateTime.UtcNow,
            RecievedCreatedAtUtc = DateTime.UtcNow,
        };
        _daoLayer.AddEvent(kafkaMessage);

        var message =
            _daoLayer.DidPreviousRelatedEntityFail(
                new TopicPartitionOffset(kafkaMessage.Topic, new Partition(kafkaMessage.Partition),
                    kafkaMessage.OffSet + 1), kafkaMessage.PartitionKey);
        message.Should().BeTrue();
    }
    
    [Test]
    public void ReturnTrueIfPreviousEntityMessageFailedToProcess_later_successfully_processed()
    {
        _db.KafkaEvents.Any().Should().BeFalse();
        var kafkaMessage = new KafkaMessage
        {
            Partition = 3,
            Topic = "asdasd",
            OffSet = long.MaxValue - 2,
            ProcessedSuccefully = false,
            ReasonText = "asds",
            PartitionKey = "fsdfdsf",
            FinishedProcessingAtUtc = DateTime.UtcNow,
            RecievedCreatedAtUtc = DateTime.UtcNow,
        };
        _daoLayer.AddEvent(kafkaMessage);
        var kafkaMessage2 = new KafkaMessage
        {
            Partition = 3,
            Topic = "asdasd",
            OffSet = long.MaxValue - 2,
            ProcessedSuccefully = true,
            ReasonText = "asds",
            PartitionKey = "fsdfdsf",
            FinishedProcessingAtUtc = DateTime.UtcNow,
            RecievedCreatedAtUtc = DateTime.UtcNow,
        };
        _daoLayer.AddEvent(kafkaMessage2);

        var message =
            _daoLayer.DidPreviousRelatedEntityFail(
                new TopicPartitionOffset(kafkaMessage.Topic, new Partition(kafkaMessage.Partition),
                    kafkaMessage.OffSet + 1), kafkaMessage.PartitionKey);
        message.Should().BeFalse();
    }

    /// <summary>
    /// Handle reprocessing of the entire stream, it will meet messages that has previously failed that might succeed now
    /// We need to store in persistence layer the history of first time it failed, second time if succeeded
    /// </summary>
    [Test]
    public void
        HandleIfOffsetHasBeenResetAndAllMessagesAreBeingReprocessed_PrevoiuslyFailedMessageAreNowProcessedCorrectly()
    {
        _db.KafkaEvents.Any().Should().BeFalse();
        
        var kafkaMessage = new KafkaMessage
        {
            Partition = 3,
            Topic = "asdasd",
            OffSet = long.MaxValue - 2,
            ProcessedSuccefully = false,
            ReasonText = "asds",
            PartitionKey = "fsdfdsf",
            FinishedProcessingAtUtc = DateTime.UtcNow,
            RecievedCreatedAtUtc = DateTime.UtcNow,
        };
        _daoLayer.AddEvent(kafkaMessage);

        var message =
            _daoLayer.DidPreviousRelatedEntityFail(
                new TopicPartitionOffset(kafkaMessage.Topic, new Partition(kafkaMessage.Partition),
                    kafkaMessage.OffSet -3), kafkaMessage.PartitionKey);
        message.Should().BeFalse();
    }
}