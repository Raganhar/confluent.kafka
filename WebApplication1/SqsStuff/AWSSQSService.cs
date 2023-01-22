using Amazon.SQS.Model;
using Newtonsoft.Json;
using nup.kafka;
using nup.kafka.Models;
using WebApplication1.Models;

namespace WebApplication1.SqsStuff;

public interface IAWSSQSService
{
    Task<bool> PostMessageAsync(User user);
    Task<bool> PostMessageAsync(string @event);
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

    public async Task<bool> PostMessageAsync(User user)
    {
        try
        {
            UserDetail userDetail = new UserDetail();
            userDetail.Id = new Random().Next(999999999);
            userDetail.FirstName = user.FirstName;
            userDetail.LastName = user.LastName;
            userDetail.UserName = user.UserName;
            userDetail.EmailId = user.EmailId;
            userDetail.CreatedOn = DateTime.UtcNow;
            userDetail.UpdatedOn = DateTime.UtcNow;
            return await _AWSSQSHelper.SendMessageAsync(userDetail);
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }
    public async Task<bool> PostMessageAsync(string @event)
    {
        try
        {
            return await _AWSSQSHelper.SendMessageAsync(@event);
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    public async Task<List<AllMessage>> GetAllMessagesAsync()
    {
        List<AllMessage> allMessages = new List<AllMessage>();
        try
        {
            List<Message> messages = await _AWSSQSHelper.ReceiveMessageAsync();
            allMessages = messages.Select(c =>
            {
                var eventName_and_Type = c.Attributes.ContainsKey(LegacySqsConsts.Event) ? c.Attributes[LegacySqsConsts.Event] : null;
                var message = new AllMessage();
                message.MessageId = c.MessageId;
                message.ReceiptHandle = c.ReceiptHandle;
                message.Payload = c.Body;
                message.Topic = eventName_and_Type;
                message.EventType = eventName_and_Type;
                message.OriginatedAt = c.Attributes.ContainsKey(KafkaConsts.OriginatedAt)? Enum.Parse<OriginatingPlatform>(c.Attributes[KafkaConsts.OriginatedAt]) :OriginatingPlatform.Sqs;
                message.Producer = c.Attributes.ContainsKey(KafkaConsts.Producer)? c.Attributes[KafkaConsts.OriginatedAt] :null;
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