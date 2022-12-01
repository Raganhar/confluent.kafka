namespace ExampleEvents;

public abstract class EventBase
{
    public abstract string Topic { get; }
    public object Payload { get; set; }
}

public abstract class PayloadEvent<T>:EventBase
{
    public abstract T Payload { get; set; }
}