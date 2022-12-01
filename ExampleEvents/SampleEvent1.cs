namespace ExampleEvents;

public class SampleEvent1 : EventBase<SampleEvent1.Data>
{
    public override string Topic { get; } = "SampleEventName";
    public override Data Payload { get; set; }

    public class Data
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }
}