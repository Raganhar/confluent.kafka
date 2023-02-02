// See https://aka.ms/new-console-template for more information


using Newtonsoft.Json;
using nup.kafka;



var options = JsonConvert.DeserializeObject<KafkaOptions>(File.ReadAllText("Secrets.json"));
options.Brokers = new List<string> { "pkc-zpjg0.eu-central-1.aws.confluent.cloud:9092" };
var manager = new KafkaWrapper("appname",options);
await manager.CreateTopic("bob",options);