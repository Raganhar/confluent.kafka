using System.Globalization;
using System.Text;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using nup.kafka;
using nup.kafka.Models;
using Serilog;
using Serilog.Context;
using WebApplication1.Config;
using WebApplication1.Models;
using WebApplication1.SqsStuff;
using MessageAttributeValue = Amazon.SQS.Model.MessageAttributeValue;

namespace WebApplication1.Workers;

public class KafkaEventShoveller : IHostedService, IDisposable
{
    private KafkaWrapperConsumer _consumer;
    private readonly IServiceScopeFactory _scope;
    private readonly IOptions<ServiceConfiguration> _configs;
    private CancellationTokenSource _cts;

    public KafkaEventShoveller(KafkaWrapperConsumer consumer, IServiceScopeFactory scope, IOptions<ServiceConfiguration> configs)
    {
        _consumer = consumer;
        _scope = scope;
        _configs = configs;
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
                        x.Key == KafkaConsts.OriginatedAt && x.GetValueBytes().AsString() ==
                        OriginatingPlatform.Kafka.ToString());
                    if (startedAtKafka)
                    {
                        Log.Information("{worker} received and processed {@event}", this.GetType().Name, e);
                        var sqsService = _scope.CreateScope().ServiceProvider.GetRequiredService<IAmazonSimpleNotificationService>();
                        var messageAttributeValues = MapMessageAttributes(e);
                        var publishRequest = new PublishRequest
                        {
                            TopicArn = _configs.Value.TopicArn,
                            MessageAttributes = messageAttributeValues,
                            Message = e.Message.Value
                        };
                        sqsService.PublishAsync(publishRequest).Wait();
                        Log.Information("{worker} send {@event} to sqs", this.GetType().Name, e);
                    }
                    else
                    {
                        Log.Information("{worker} event started on sqs, no need to send it back", this.GetType().Name);
                    }
                }, "^.*");
                // }, "^[a-z][a-zA-Z].*");
            }
            catch (Exception e)
            {
                Log.Error("unhandled exception when trying to consume {@exception}", e);
            }
        }

        return Task.CompletedTask;
    }

    private static Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
        MapMessageAttributes(ConsumeResult<Ignore, string> consumeResult)
    {
        var messageAttributeValues = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>();
        MapLegacyProperties(consumeResult, messageAttributeValues);
        foreach (var messageHeader in consumeResult.Message.Headers)
        {
            messageAttributeValues.Add(messageHeader.Key, new Amazon.SimpleNotificationService.Model.MessageAttributeValue()
            {
                StringValue = messageHeader.GetValueBytes().AsString(),
                DataType = LegacySqsConsts.String
            });     
        }
        return messageAttributeValues;
    }

    private static void MapLegacyProperties(ConsumeResult<Ignore, string> consumeResult, Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue> messageAttributeValues)
    {
        messageAttributeValues.Add("Event", new Amazon.SimpleNotificationService.Model.MessageAttributeValue()
        {
            StringValue = consumeResult.Message.Headers.Single(x => x.Key == KafkaConsts.EventType).GetValueBytes()
                .AsString(),
            DataType = LegacySqsConsts.String
        });
        var timestampAsLong = DateTimeOffset
            .ParseExact(
                consumeResult.Message.Headers.Single(x => x.Key == KafkaConsts.CreatedAt).GetValueBytes().AsString(),
                KafkaConsts.DatetimeFOrmat, CultureInfo.InvariantCulture.DateTimeFormat).ToUnixTimeSeconds().ToString();
        messageAttributeValues.Add("TimeStamp", new Amazon.SimpleNotificationService.Model.MessageAttributeValue()
        {
            StringValue = timestampAsLong,
            DataType = LegacySqsConsts.String
        });
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