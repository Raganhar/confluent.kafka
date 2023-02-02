using KafkaAndSqsShoveller.SqsStuff;
using WebApplication1.Models;

namespace WebApplication1;

public class SqsStuff
{
    private readonly IAWSSQSHelper _AWSSQSHelper;

    public SqsStuff(IAWSSQSHelper AWSSQSHelper)
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
            return await _AWSSQSHelper.SendMessageAsync(userDetail, userDetail.GetType().FullName);
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }
}