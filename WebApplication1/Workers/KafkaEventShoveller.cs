using System.Text;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Confluent.Kafka;
using nup.kafka;
using nup.kafka.Models;
using Serilog;
using Serilog.Context;
using WebApplication1.Models;
using WebApplication1.SqsStuff;
using MessageAttributeValue = Amazon.SQS.Model.MessageAttributeValue;

namespace WebApplication1.Workers;

public class KafkaEventShoveller : IHostedService, IDisposable
{
    private KafkaWrapperConsumer _consumer;
    private readonly IServiceScopeFactory _scope;
    private CancellationTokenSource _cts;

    public KafkaEventShoveller(KafkaWrapperConsumer consumer, IServiceScopeFactory scope)
    {
        _consumer = consumer;
        _scope = scope;
        _cts = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        using (LogContext.PushProperty("worker", "KafkaEventShoveller"))
        {
            try
            {
                _consumer.Consume(_cts.Token, (ConsumeResult<Ignore, string> e) =>
                {
                    var startedAtKafka = e.Message.Headers.Any(x =>
                        x.Key == KafkaConsts.OriginatedAt && Encoding.UTF8.GetString(x.GetValueBytes()) ==
                        OriginatingPlatform.Kafka.ToString());
                    if (startedAtKafka)
                    {
                        Log.Information("{worker} received and processed {@event}", this.GetType().Name, e);
                        var sqsService = _scope.CreateScope().ServiceProvider.GetRequiredService<IAmazonSimpleNotificationService>();
                        var messageAttributeValues = MapMessageAttributes();
                        var publishRequest = new PublishRequest
                        {
                            TopicArn = "arn:aws:sns:eu-north-1:254403012497:AWS_SNS_DEMO",
                            MessageAttributes = messageAttributeValues,
                            Message = ""
                        };
                        sqsService.PublishAsync(publishRequest);
                        Log.Information("{worker} send {@event} to sqs", this.GetType().Name, e);
                    }
                    else
                    {
                        Log.Information("{worker} event started on sqs, no need to send it back", this.GetType().Name);
                    }
                }, "*");
            }
            catch (Exception e)
            {
                Log.Error("unhandled exception when trying to consume {@exception}", e);
            }
        }

        return Task.CompletedTask;
    }

    private static Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue> MapMessageAttributes()
    {
        var messageAttributeValues = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>();
        messageAttributeValues.Add("Event", new Amazon.SimpleNotificationService.Model.MessageAttributeValue()
        {
            StringValue = ""
        });
        return messageAttributeValues;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}