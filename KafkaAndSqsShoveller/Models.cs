using nup.kafka.Models;

namespace KafkaAndSqsShoveller;

public class DeleteMessage
{
    public string ReceiptHandle { get; set; }
}
public class AllMessage
{
    public string MessageId { get; set; }
    public string Payload { get; set; }
    public string ReceiptHandle { get; set; }
    public string Topic { get; set; }
    public string EntityKey { get; set; }
    public string Producer { get; set; }
    public string EventType { get; set; }
    public OriginatingPlatform OriginatedAt { get; set; }
}
