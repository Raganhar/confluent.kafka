using Amazon.SQS.Model;
using nup.kafka;
using nup.kafka.Models;

namespace KafkaAndSqsShoveller.SqsStuff;

public interface IAWSSQSService
{
    // Task<bool> PostMessageAsync(User user);
    // Task<bool> PostMessageAsync(string @event, string eventType);
    Task<List<AllMessage>> GetAllMessagesAsync();
    Task<bool> DeleteMessageAsync(DeleteMessage deleteMessage);
}

public class AWSSQSService : IAWSSQSService
{
    private readonly IAWSSQSHelper _AWSSQSHelper;

    public AWSSQSService(IAWSSQSHelper AWSSQSHelper)
    {
        this._AWSSQSHelper = AWSSQSHelper;
    }

    public async Task<List<AllMessage>> GetAllMessagesAsync()
    {
        List<AllMessage> allMessages = new List<AllMessage>();
        try
        {
            List<Message> messages = await _AWSSQSHelper.ReceiveMessageAsync();
            allMessages = messages.Select(c =>
            {
                var attributes = c.MessageAttributes;
                var eventName_and_Type = attributes.ContainsKey(LegacySqsConsts.Event) ? attributes[LegacySqsConsts.Event].StringValue : null;
                var message = new AllMessage();
                message.MessageId = c.MessageId;
                message.ReceiptHandle = c.ReceiptHandle;
                message.Payload = c.Body;
                message.Topic = eventName_and_Type;
                message.EventType = eventName_and_Type;
                message.OriginatedAt = attributes.ContainsKey(KafkaConsts.OriginatedAt)? Enum.Parse<OriginatingPlatform>(attributes[KafkaConsts.OriginatedAt].StringValue) :OriginatingPlatform.Sqs;
                message.Producer = attributes.ContainsKey(KafkaConsts.Producer)? attributes[KafkaConsts.OriginatedAt].StringValue :null;
                return message;
            }).ToList();
            return allMessages;
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    public async Task<bool> DeleteMessageAsync(DeleteMessage deleteMessage)
    {
        try
        {
            return await _AWSSQSHelper.DeleteMessageAsync(deleteMessage.ReceiptHandle);
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }
}