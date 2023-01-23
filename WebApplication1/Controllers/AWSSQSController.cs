using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using KafkaAndSqsShoveller;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using nup.kafka;
using WebApplication1.Config;
using WebApplication1.Models;
using WebApplication1.SqsStuff;

namespace WebApplication1.Controllers;

[Produces("application/json")]
[Route("api/[controller]")]
[ApiController]
public class AWSSQSController : ControllerBase
{
    private readonly IAWSSQSService _AWSSQSService;
    private readonly ServiceConfiguration _configs;

    public AWSSQSController(IAWSSQSService AWSSQSService, IOptions<ServiceConfiguration> configs)
    {
        this._AWSSQSService = AWSSQSService;
        _configs = configs.Value;
    }

    [Route("postMessage")]
    [HttpPost]
    public async Task<IActionResult> PostMessageAsync([FromServices] IAmazonSimpleNotificationService sns,
        [FromBody] User user)
    {
        var publishRequest = new PublishRequest
        {
            TopicArn = _configs.TopicArn,
            MessageAttributes = new Dictionary<string, MessageAttributeValue>
            {
                {
                    LegacySqsConsts.Event, new MessageAttributeValue
                    {
                        StringValue = user.GetType().FullName,
                        DataType = "String"
                    }
                },
                {
                    LegacySqsConsts.Timestamp, new MessageAttributeValue
                    {
                        StringValue = DateTimeOffset.Now.ToUnixTimeSeconds().ToString(),
                        DataType = "String"
                    }
                }
            },
            Message = JsonConvert.SerializeObject(user)
        };
        // var publishAsync = await sns.PublishAsync(publishRequest);

        var postMessageAsync = await _AWSSQSService.PostMessageAsync(user);
        return Ok(postMessageAsync);
        // return StatusCode((int)publishAsync.HttpStatusCode);
    }

    
    [Route("postToKafka")]
    [HttpPost]
    public async Task<IActionResult> PostToKafkaAsync([FromServices] KafkaWrapper kafka,
        [FromBody] User user)
    {
        await kafka.Send(user);
        // var publishAsync = await sns.PublishAsync(publishRequest);

        return Ok(true);
        // return StatusCode((int)publishAsync.HttpStatusCode);
    }

    [Route("getAllMessages")]
    [HttpGet]
    public async Task<IActionResult> GetAllMessagesAsync()
    {
        var result = await _AWSSQSService.GetAllMessagesAsync();
        return Ok(result);
    }

    [Route("deleteMessage")]
    [HttpDelete]
    public async Task<IActionResult> DeleteMessageAsync(DeleteMessage deleteMessage)
    {
        var result = await _AWSSQSService.DeleteMessageAsync(deleteMessage);
        return Ok(new { isSucess = result });
    }
}