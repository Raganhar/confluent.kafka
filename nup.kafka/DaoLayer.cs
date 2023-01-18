using Confluent.Kafka;
using nup.kafka.DatabaseStuff;

namespace nup.kafka;

public class DaoLayer
{
    private readonly KafkaMysqlDbContext _db;

    public DaoLayer(KafkaMysqlDbContext db)
    {
        _db = db;
    }

    public void AddEvent(KafkaMessage kafkaMessage)
    {
        _db.KafkaEvents.Add(kafkaMessage);
        _db.SaveChanges();
    }

    public KafkaMessage Get(TopicPartitionOffset TopicPartitionOffset, string? partitionKey)
    {
        if (TopicPartitionOffset == null) throw new ArgumentNullException(nameof(TopicPartitionOffset));
        var previouslyProcessedEvent = _db.KafkaEvents.FirstOrDefault(x =>
            x.Topic == TopicPartitionOffset.Topic && 
            x.Partition == TopicPartitionOffset.Partition.Value &&
            x.OffSet == TopicPartitionOffset.Offset.Value &&
            x.ProcessedSuccefully == true);
        return previouslyProcessedEvent;
    }
    public bool DidPreviousRelatedEntityFail(TopicPartitionOffset TopicPartitionOffset, string? partitionKey)
    {
        if (TopicPartitionOffset == null) throw new ArgumentNullException(nameof(TopicPartitionOffset));
        var previouslyProcessedEvent = _db.KafkaEvents.Where(x =>
            x.Topic == TopicPartitionOffset.Topic &&
            x.PartitionKey == partitionKey &&
            x.Partition == TopicPartitionOffset.Partition.Value &&
            x.OffSet< TopicPartitionOffset.Offset.Value
            // x.Partition == TopicPartitionOffset.Partition.Value &&
            // x.OffSet == TopicPartitionOffset.Offset &&
            ).OrderByDescending(x=>x.RecievedCreatedAtUtc).FirstOrDefault();
        return previouslyProcessedEvent?.ProcessedSuccefully == false;
    }
}