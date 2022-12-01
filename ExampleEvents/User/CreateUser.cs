namespace ExampleEvents.User;

public class CreateUser<T>:UserAggregate<T>
{
    public override T Payload { get; set; }
    public override string EventType { get; } = "CreateUser";
}