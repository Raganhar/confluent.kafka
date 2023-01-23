using System.Globalization;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Confluent.Kafka;
using nup.kafka;
using nup.kafka.Models;
using Serilog;
using Serilog.Context;

namespace KafkaAndSqsShoveller;

public class ShovelToSqs
{
    private readonly KafkaWrapperConsumer _consumer;

    public ShovelToSqs(KafkaWrapperConsumer consumer)
    {
        _consumer = consumer;
    }
    
    public void PushToSqsShovel(CancellationToken cancellationToken, Func<IAmazonSimpleNotificationService> GetSnsClient, string topicArn)
    {
        using (LogContext.PushProperty("worker", "KafkaEventShoveller"))
        {
            try
            {
                _consumer.Consume(cancellationToken, (ConsumeResult<Ignore, string> e) =>
                {
                    var startedAtKafka = e.Message.Headers.Any(x =>
                        x.Key == KafkaConsts.OriginatedAt && x.GetValueBytes().AsString() ==
                        OriginatingPlatform.Kafka.ToString());
                    if (startedAtKafka)
                    {
                        Log.Information("{worker} received and processed {@event}", this.GetType().Name, e);
                        var snsClient = GetSnsClient();
                        var messageAttributeValues = MapMessageAttributes(e);
                        var publishRequest = new PublishRequest
                        {
                            TopicArn = topicArn,
                            MessageAttributes = messageAttributeValues,
                            Message = e.Message.Value
                        };
                        snsClient.PublishAsync(publishRequest).Wait();
                        Log.Information("{worker} send {@event} to sqs", this.GetType().Name, e);
                    }
                    else
                    {
                        Log.Information("{worker} event started on sqs, no need to send it back", this.GetType().Name);
                    }
                }, "^.*");
            }
            catch (Exception e)
            {
                Log.Error("unhandled exception when trying to consume {@exception}", e);
            }
        }
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
}