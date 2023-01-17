using ExampleEvents;

namespace nup.kafka.tests;

public class CompetingConsumersTests
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
    public async Task CompetingConsumers()
    {
        try
        {
            var _consumer1 = new KafkaWrapperConsumer(TestConsts.brokers, "TestApp","consumer1");
            var _consumer2 = new KafkaWrapperConsumer(TestConsts.brokers, "TestApp","consumer2");

            _consumer1.Consume(new CancellationToken(),(SampleEvent1 e)=> Console.WriteLine($"Handler1 received: {e.Name}") );
            _consumer2.Consume(new CancellationToken(),(SampleEvent1 e)=> Console.WriteLine($"Handler2 received: {e.Name}") );
            
            foreach (var i in Enumerable.Range(0,100))
            {
                await _client.Send(new SampleEvent1
                {
                    Age = i,
                    Name = "bobsilol"
                });   
            }

            await Task.Delay(3000);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}