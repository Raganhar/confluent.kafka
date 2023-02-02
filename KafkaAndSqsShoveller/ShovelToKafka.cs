using KafkaAndSqsShoveller.SqsStuff;
using nup.kafka;
using nup.kafka.Models;
using Serilog;
using Serilog.Context;

namespace KafkaAndSqsShoveller;

public class ShovelToKafka
{
    private readonly KafkaWrapper _kafkaClient;

    public ShovelToKafka(KafkaWrapper kafkaClient)
    {
        _kafkaClient = kafkaClient;
    }
    
    public void PushToKafka(CancellationToken _cts, Func<IAWSSQSService> func)
    {
        using (LogContext.PushProperty("worker", "sqsWorker"))
        {
            ThreadPool.QueueUserWorkItem(state =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    Log.Information("Retrieving SQS messages");
                    var sqs =func();
                    var allMessagesAsync = sqs.GetAllMessagesAsync();
                    Task.WaitAll(allMessagesAsync);
                    var allMessages = allMessagesAsync.Result;
                    Log.Information("found {msgCount}", allMessages.Count);
                    allMessages.ForEach(x =>
                    {
                        try
                        {
                            if (x.OriginatedAt == OriginatingPlatform.Sqs)
                            {
                                Log.Information("sending msg to kafka");
                                _kafkaClient.Send(x.Payload, x.Topic, x.EventType, OriginatingPlatform.Sqs, x.EntityKey).Wait();
                                Log.Information("sendt msg to kafka");
                            }
                            else
                            {
                                Log.Information("Message originated on Kafka, wont send it back on kafka");
                            }

                            sqs.DeleteMessageAsync(new DeleteMessage
                            {
                                ReceiptHandle = x.ReceiptHandle
                            });
                        }
                        catch (Exception e)
                        {
                            Log.Error(e, "Forwarding sqs message to Kafka failed");
                        }
                    });
                }
            }, "sqsworker");
        }
    }

}