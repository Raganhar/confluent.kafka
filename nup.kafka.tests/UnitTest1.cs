using ExampleEvents;

namespace nup.kafka.tests;

public class Tests
{
    private KafkaWrapper _client;

    [SetUp]
    public void Setup()
    {
        _client = new KafkaWrapper(TestConsts.brokers, "TestApp", new ProducerOptions
        {
            PartitionCount = 3
        });
    }

    [Test]
    public async Task Test1()
    {
        try
        {
            ThreadPool.QueueUserWorkItem(state => _client.Consume(), "ThreadPool");
            await _client.Send(new SampleEvent1
            {
                Age = 3,
                Name = "bobsilol"
            });
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}