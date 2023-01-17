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
            ThreadPool.QueueUserWorkItem(state => KafkaWrapper.Run_Consume(TestConsts.brokers,new List<string>()
                {
                    typeof(SampleEvent1).FullName
                },new CancellationToken()), "ThreadPool");
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