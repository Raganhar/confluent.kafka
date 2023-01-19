using System.Collections.Concurrent;
using System.Security.Principal;
using ExampleEvents;
using FluentAssertions;
using Newtonsoft.Json;
using NUnit.Framework.Internal;
using nup.kafka.DatabaseStuff;
using Serilog;

namespace nup.kafka.tests;

public class EnsurePartitionOrderingByEntityIdTests : TestFixture
{
    private KafkaWrapper _client;

    [SetUp]
    public void Setup()
    {
        _client = new KafkaWrapper(TestConsts.brokers, "TestApp", new ProducerOptions
        {
            PartitionCount = 4
        });
    }

    [Test]
    public async Task CompetingConsumers()
    {
        void EventHandler(ConcurrentDictionary<string, List<int>> processedEvents, SampleEvent1 e, string processer)
        {
            var v = processedEvents.GetOrAdd($"{processer}_{e.Name}", new List<int>(0));
            v.Add(e.Age);
            Console.WriteLine($"{processer} received: {e.Name}");
        }

        try
        {
            var _consumer1 = new KafkaWrapperConsumer(TestConsts.brokers, "TestApp", "consumer1").WithDatabase(KafkaMysqlDbContext.ConnectionString);
            var _consumer2 = new KafkaWrapperConsumer(TestConsts.brokers, "TestApp","consumer2").WithDatabase(KafkaMysqlDbContext.ConnectionString);

            var processedEvents = new ConcurrentDictionary<string, List<int>>();

            _consumer1.Consume(new CancellationToken(),
                (SampleEvent1 e) => { EventHandler(processedEvents, e, "handler1"); });
            _consumer2.Consume(new CancellationToken(),
                (SampleEvent1 e) => { EventHandler(processedEvents, e, "handler2"); });

            var uid1 = Guid.NewGuid();
            var uid2 = Guid.NewGuid();
            await Task.Delay(3000);
            var guids = Enumerable.Range(0, 10).Select(x => Guid.NewGuid()).ToList();
            foreach (var i in Enumerable.Range(1, 3))
            {
                foreach (var x in guids)
                {
                    await _client.Send(new SampleEvent1
                    {
                        Age = i,
                        Name = x.ToString()
                    }, x.ToString());
                    // },(b==1?uid1:id2).ToString());
                }
            }

            await Task.Delay(3000);
            // processedEvents.Values.Sum().Should().Be(10);
            processedEvents.Keys.Count.Should().Be(10);
            foreach (var processedEventsKey in processedEvents.Keys)
            {
                processedEvents[processedEventsKey].Sum().Should().BeGreaterThan(0);
            }

            Console.WriteLine($"Data: {JsonConvert.SerializeObject(processedEvents, Formatting.Indented)}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}