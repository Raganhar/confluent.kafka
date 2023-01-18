// See https://aka.ms/new-console-template for more information

using Newtonsoft.Json;
using nup.kafka;
using nup.kafka.tests;

Console.WriteLine("Hello, World!");
new KafkaWrapperConsumer(TestConsts.brokers,"console-app").Consume(CancellationToken.None, (ExampleEvents.SampleEvent1 e)=> Console.WriteLine(JsonConvert.SerializeObject(e)));
Console.ReadKey();