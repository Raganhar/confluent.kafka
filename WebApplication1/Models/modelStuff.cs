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

