namespace ExampleEvents;


public abstract class EventBase<T>
{
    public abstract string Topic { get; }
    public abstract T Payload { get; set; }
}

public abstract class Aggregate<T> : EventBase<T>
{
    public abstract string EventType { get; }
}