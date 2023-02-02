using Confluent.Kafka;

namespace nup.kafka;

public static class KafkaUtils
{
    public static ClientConfig InitConfig(KafkaOptions defaultKafkaOptions)
    {
        return new ClientConfig
        {
            BootstrapServers = string.Join(",", defaultKafkaOptions.Brokers), SaslUsername = defaultKafkaOptions.ConfluentUsername,
            SaslPassword = defaultKafkaOptions.ConfluentPassword, SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
        };
    }
}