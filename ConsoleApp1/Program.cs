// See https://aka.ms/new-console-template for more information

using Newtonsoft.Json;
using nup.kafka;
using nup.kafka.DatabaseStuff;
using nup.kafka.tests;

Console.WriteLine("Hello, World!");
var consumerOptions = new KafkaOptions()
{
    AppName = "TestApp",
    Brokers = TestConsts.brokers
};
new KafkaWrapperConsumer(consumerOptions,  new EventProcesser(), KafkaMysqlDbContext.ConnectionString).Consume(CancellationToken.None, (ExampleEvents.SampleEvent1 e)=> Console.WriteLine(JsonConvert.SerializeObject(e)));
Console.ReadKey();