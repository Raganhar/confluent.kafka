using ExampleEvents;
using nup.kafka.DatabaseStuff;

namespace nup.kafka.tests;

public class Tests
{
    private KafkaWrapper _client;
    private KafkaWrapperConsumer _consumer;

    [SetUp]
    public void Setup()
    {
        _client = new KafkaWrapper(TestConsts.brokers, "TestApp", new ProducerOptions
        {
            PartitionCount = 4
        });
        _consumer = new KafkaWrapperConsumer(TestConsts.brokers, "TestApp", KafkaMysqlDbContext.ConnectionString);
    }

    [Test]
    public async Task SimpleCase()
    {
        try
        {
            _consumer.Consume(new CancellationToken(),(SampleEvent1 e)=> Console.WriteLine($"Handler received: {e.Name}") );
            await _client.Send(new SampleEvent1
            {
                Age = 3,
                Name = "bobsilol"
            });
            await Task.Delay(3000);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}