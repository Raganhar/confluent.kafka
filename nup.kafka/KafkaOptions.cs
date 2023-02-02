namespace nup.kafka;

public class KafkaOptions
{
    public int PartitionCount { get; set; } = 1;
    public string ConfluentUsername { get; set; }
    public string ConfluentPassword { get; set; }
    public string AppName { get; set; }
    public List<string> Brokers { get; set; }
}