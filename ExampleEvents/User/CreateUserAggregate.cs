namespace ExampleEvents.User;

public abstract class UserAggregate<T>:Aggregate<T>
{
    public override string Topic { get; } = "User";
}