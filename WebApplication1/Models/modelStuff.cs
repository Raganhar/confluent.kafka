using nup.kafka;
using nup.kafka.Models;

namespace WebApplication1.Models;

public class User
{
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string UserName { get; set; }
    public string EmailId { get; set; }
}

public class UserDetail : User
{
    public int Id { get; set; }
    public DateTime CreatedOn { get; set; }
    public DateTime UpdatedOn { get; set; }
}

public class AllMessage
{
    public string MessageId { get; set; }
    public string Payload { get; set; }
    public string ReceiptHandle { get; set; }
    public string Topic { get; set; }
    public string EntityKey { get; set; }
    public string Producer { get; set; }
    public string EventType { get; set; }
    public OriginatingPlatform OriginatedAt { get; set; }
}


public class DeleteMessage
{
    public string ReceiptHandle { get; set; }
}