using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace nup.kafka.DatabaseStuff;

public class KafkaMessage
{
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Key]
    public Guid Id { get; set; }
    [StringLength(255)]
    public string? PartitionKey { get; set; }
    public int Partition { get; set; }
    public long OffSet { get; set; }
    public bool ProcessedSuccefully { get; set; }
    [StringLength(2000)]
    public string? ReasonText { get; set; }
    public string Topic { get; set; }
    public string ConsumerGroupId { get; set; }
    public DateTime RecievedCreatedAtUtc { get; set; }
    public DateTime? FinishedProcessingAtUtc { get; set; }
}